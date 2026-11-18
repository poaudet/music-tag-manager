#!/usr/bin/env python3
"""Tag staging: scan files into SQLite, query MusicBrainz, write tags from views."""
import argparse, json, os, shutil, sqlite3, time
from datetime import datetime, timezone
from pathlib import Path

import mutagen
import requests
from mutagen.id3 import ID3, ID3NoHeaderError, TXXX
from mutagen.mp4 import MP4, MP4FreeForm, AtomDataType

EXT = {".mp3": "id3", ".flac": "vorbis", ".ogg": "vorbis",
       ".opus": "vorbis", ".m4a": "mp4", ".mp4": "mp4"}
MB = "https://musicbrainz.org/ws/2"
UA = os.environ.get("MB_USER_AGENT", "tag-staging/0.1 (poaudet@mbtag.tk)")

# normalized tag -> native key per container (writable tags)
MB_TAGS = {
    "mb_release_id": {"id3": "MusicBrainz Album Id", "vorbis": "MUSICBRAINZ_ALBUMID",
                      "mp4": "----:com.apple.iTunes:MusicBrainz Album Id"},
    "mb_release_group_id": {"id3": "MusicBrainz Release Group Id", "vorbis": "MUSICBRAINZ_RELEASEGROUPID",
                            "mp4": "----:com.apple.iTunes:MusicBrainz Release Group Id"},
    "mb_album_artist_id": {"id3": "MusicBrainz Album Artist Id", "vorbis": "MUSICBRAINZ_ALBUMARTISTID",
                           "mp4": "----:com.apple.iTunes:MusicBrainz Album Artist Id"},
}
# read-only context used for the MusicBrainz search
INFO = {
    "album": {"id3": "TALB", "vorbis": "album", "mp4": "\xa9alb"},
    "albumartist": {"id3": "TPE2", "vorbis": "albumartist", "mp4": "aART"},
    "artist": {"id3": "TPE1", "vorbis": "artist", "mp4": "\xa9ART"},
}
# how to pull each tag out of a MusicBrainz release JSON
MB_EXTRACT = {
    "mb_release_id": lambda r: r["id"],
    "mb_release_group_id": lambda r: r["release-group"]["id"],
    "mb_album_artist_id": lambda r: r["artist-credit"][0]["artist"]["id"],
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS files(
  path TEXT PRIMARY KEY, folder TEXT, fmt TEXT,
  mtime REAL, size INTEGER, missing INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS tags(
  path TEXT, tag TEXT, value TEXT, PRIMARY KEY(path, tag));
CREATE TABLE IF NOT EXISTS overrides(
  path TEXT, tag TEXT, value TEXT, PRIMARY KEY(path, tag));
CREATE TABLE IF NOT EXISTS mb_candidates(
  path TEXT, tag TEXT, mb_value TEXT, release_id TEXT,
  confidence REAL, mb_track_count INTEGER, fetched_at TEXT, group_id TEXT,
  PRIMARY KEY(path, tag, release_id));
CREATE TABLE IF NOT EXISTS tag_config(
  tag TEXT PRIMARY KEY, enabled INTEGER, policy TEXT);

CREATE VIEW IF NOT EXISTS folder_status AS
SELECT f.folder, COUNT(*) AS n_files,
       COUNT(DISTINCT t.value) AS n_ids,
       SUM(t.value IS NULL) AS n_missing,
       CASE WHEN COUNT(DISTINCT t.value) = 0 THEN 'unmatched'
            WHEN COUNT(DISTINCT t.value) > 1 THEN 'mixed'
            WHEN SUM(t.value IS NULL) > 0 THEN 'partial'
            ELSE 'clean' END AS status
FROM files f
LEFT JOIN tags t ON t.path = f.path AND t.tag = 'mb_release_id'
WHERE f.missing = 0
GROUP BY f.folder;

-- Default strategy (replace by hand): fill missing tags from the best MB candidate
CREATE VIEW IF NOT EXISTS tags_to_update AS
WITH best AS (
  SELECT c.*, ROW_NUMBER() OVER (PARTITION BY path, tag ORDER BY confidence DESC) AS rn
  FROM mb_candidates c)
SELECT b.path AS file_path, b.tag AS tag, t.value AS current_value,
       COALESCE(o.value, b.mb_value) AS wanted_value
FROM best b
JOIN files f ON f.path = b.path AND f.missing = 0
LEFT JOIN tags t ON t.path = b.path AND t.tag = b.tag
LEFT JOIN overrides o ON o.path = b.path AND o.tag = b.tag
JOIN tag_config tc ON tc.tag = b.tag AND tc.enabled = 1 AND tc.policy = 'fill_if_missing'
WHERE b.rn = 1 AND b.confidence >= 0.85
  AND (t.value IS NULL OR t.value = '')
  AND COALESCE(o.value, b.mb_value) <> '';

CREATE VIEW IF NOT EXISTS files_to_update AS
SELECT DISTINCT file_path FROM tags_to_update;
"""


# ---------- tag IO ----------
def _s(x):
    return x.decode("utf-8", "replace") if isinstance(x, bytes) else str(x)


def read_tags(path, fmt):
    out, native = {}, {**INFO, **MB_TAGS}
    if fmt == "id3":
        try:
            t = ID3(path)
        except ID3NoHeaderError:
            return out
        for tag, k in INFO.items():
            f = t.get(k["id3"])
            if f is not None and f.text:
                out[tag] = _s(f.text[0])
        txxx = {f.desc.lower(): _s(f.text[0]) for f in t.getall("TXXX") if f.text}
        for tag, k in MB_TAGS.items():
            if txxx.get(k["id3"].lower()):
                out[tag] = txxx[k["id3"].lower()]
    else:
        f = mutagen.File(path) if fmt == "vorbis" else MP4(path)
        tags = f.tags if f is not None and f.tags is not None else {}
        for tag, k in native.items():
            v = tags.get(k[fmt])
            if v and _s(v[0]):
                out[tag] = _s(v[0])
    return out


def write_file(path, fmt, values):
    if fmt == "id3":
        try:
            t = ID3(path)
        except ID3NoHeaderError:
            t = ID3()
        for tag, v in values.items():
            desc = MB_TAGS[tag]["id3"]
            t.delall("TXXX:" + desc)
            t.add(TXXX(encoding=3, desc=desc, text=[v]))
        t.save(path)
    elif fmt == "vorbis":
        f = mutagen.File(path)
        for tag, v in values.items():
            f[MB_TAGS[tag]["vorbis"]] = [v]
        f.save()
    else:
        f = MP4(path)
        for tag, v in values.items():
            f[MB_TAGS[tag]["mp4"]] = [MP4FreeForm(v.encode(), dataformat=AtomDataType.UTF8)]
        f.save()


# ---------- db ----------
def connect(db):
    Path(db).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(conn, cfg_path):
    conn.executescript(SCHEMA)
    if not Path(cfg_path).exists():
        Path(cfg_path).write_text(json.dumps(
             {t: {"enabled": False, "policy": "fill_if_missing"} for t in MB_EXTRACT}, indent=2))
        print(f"Created {cfg_path}: set the tags you want to true")
    sync_config(conn, cfg_path)
    print("DB ready")


def index_file(conn, root, rel, fmt):
    p = root / rel
    st, tags = p.stat(), read_tags(p, fmt)
    conn.execute(
        """INSERT INTO files(path, folder, fmt, mtime, size, missing) VALUES(?,?,?,?,?,0)
           ON CONFLICT(path) DO UPDATE SET folder=excluded.folder, fmt=excluded.fmt,
           mtime=excluded.mtime, size=excluded.size, missing=0""",
        (rel, Path(rel).parent.as_posix(), fmt, st.st_mtime, st.st_size))
    conn.execute("DELETE FROM tags WHERE path=?", (rel,))
    conn.executemany("INSERT INTO tags VALUES(?,?,?)", [(rel, k, v) for k, v in tags.items()])
    return tags


def rescan(conn, root):
    conn.execute("UPDATE files SET missing=1")
    n_new = n_skip = 0
    for p in root.rglob("*"):
        fmt = EXT.get(p.suffix.lower())
        if not fmt or not p.is_file():
            continue
        rel, st = p.relative_to(root).as_posix(), p.stat()
        row = conn.execute("SELECT mtime, size FROM files WHERE path=?", (rel,)).fetchone()
        if row and row[0] == st.st_mtime and row[1] == st.st_size:
            conn.execute("UPDATE files SET missing=0 WHERE path=?", (rel,))
            n_skip += 1
            continue
        try:
            index_file(conn, root, rel, fmt)
            n_new += 1
        except Exception as e:
            print(f"[SCAN ERROR] {rel}: {e}")
    conn.commit()
    gone = conn.execute("SELECT COUNT(*) FROM files WHERE missing=1").fetchone()[0]
    print(f"Rescan: {n_new} indexed, {n_skip} unchanged, {gone} missing")


# ---------- MusicBrainz ----------
S = requests.Session()
S.headers["User-Agent"] = UA


def _get(url, params):
    r = S.get(url, params={**params, "fmt": "json"}, timeout=30)
    time.sleep(1.1)  # MB rate limit
    r.raise_for_status()
    return r.json()


def _esc(s):
    return s.replace("\\", "\\\\").replace('"', '\\"')


def mb_search(album, artist):
    q = f'release:"{_esc(album)}"' + (f' AND artist:"{_esc(artist)}"' if artist else "")
    return [(r, r.get("score", 0) / 100) for r in _get(f"{MB}/release", {"query": q, "limit": 3})["releases"]]


def mb_lookup(release_id):
    r = _get(f"{MB}/release/{release_id}", {"inc": "release-groups artist-credits"})
    return [(r, 1.0)]


def track_count(r):
    return r.get("track-count") or sum(m.get("track-count", 0) for m in r.get("media", []))

def migrate(conn):
    cols = [r[1] for r in conn.execute("PRAGMA table_info(mb_candidates)")]
    if "group_id" not in cols:
        conn.execute("ALTER TABLE mb_candidates ADD COLUMN group_id TEXT")

def load_cfg(path):
    cfg = {}
    for tag, v in json.loads(Path(path).read_text()).items():
        if tag not in MB_EXTRACT:
            print(f"[CONFIG] unknown tag ignored: {tag}")
            continue
        if isinstance(v, bool):
            v = {"enabled": v}
        policy = v.get("policy", "fill_if_missing")
        if policy not in ("fill_if_missing", "overwrite"):
            print(f"[CONFIG] bad policy for {tag}, using fill_if_missing")
            policy = "fill_if_missing"
        cfg[tag] = (int(bool(v.get("enabled"))), policy)
    return cfg

def sync_config(conn, path):
    if not Path(path).exists():
        return
    conn.execute("DELETE FROM tag_config")
    conn.executemany("INSERT INTO tag_config VALUES(?,?,?)",
                     [(t, e, p) for t, (e, p) in load_cfg(path).items()])
    conn.commit()


def mb_group_releases(gid):
    out, offset = [], 0
    while True:
        d = _get(f"{MB}/release", {"release-group": gid, "inc": "media", "limit": 100, "offset": offset})
        out += d["releases"]
        offset += 100
        if offset >= d.get("release-count", 0):
            return out

def mb_query(conn, refresh=False):
    cfg = dict(conn.execute("SELECT tag, policy FROM tag_config WHERE enabled=1"))
    if not cfg:
        return print("No tags enabled in config")
    if refresh:
        conn.executemany("DELETE FROM mb_candidates WHERE tag=?", [(t,) for t in cfg])
    need = {}
    for tag, policy in cfg.items():
        sql = ("SELECT path FROM files WHERE missing=0 "
               "AND path NOT IN (SELECT path FROM mb_candidates WHERE tag=?)")
        if policy != "overwrite":  # overwrite also covers files that already have the tag
            sql += " AND path NOT IN (SELECT path FROM tags WHERE tag=?)"
        for (p,) in conn.execute(sql, (tag,) if policy == "overwrite" else (tag, tag)):
            need.setdefault(p, []).append(tag)
    groups = {}
    for path in need:
        info = dict(conn.execute(
            "SELECT tag, value FROM tags WHERE path=? AND tag IN ('album','albumartist','artist','mb_release_id')",
            (path,)))
        if info.get("mb_release_id"):
            key = ("id", info["mb_release_id"])
        elif info.get("album"):
            key = ("search", info["album"], info.get("albumartist") or info.get("artist"))
        else:
            print(f"[SKIP] no album tag: {path}")
            continue
        groups.setdefault(key, []).append(path)

    sibs = {}  # release-group id -> sibling releases (one fetch per group)

    def add(path, tag, val, rid, conf, tc, now, gid):
        conn.execute("""INSERT OR REPLACE INTO mb_candidates
            (path, tag, mb_value, release_id, confidence, mb_track_count, fetched_at, group_id)
            VALUES(?,?,?,?,?,?,?,?)""", (path, tag, val, rid, conf, tc, now, gid))

    for key, paths in groups.items():
        print(f"[MB] {key[1:]} ({len(paths)} files)")
        try:
            results = mb_lookup(key[1]) if key[0] == "id" else mb_search(key[1], key[2])
        except Exception as e:
            print(f"  -> error: {e}")
            continue
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        for r, conf in results:
            gid = r.get("release-group", {}).get("id")
            for path in paths:
                for tag in need[path]:
                    if tag == "mb_release_id" and cfg[tag] == "overwrite" and key[0] == "id" and gid:
                        if gid not in sibs:
                            sibs[gid] = mb_group_releases(gid)
                        for s in sibs[gid]:
                            add(path, tag, s["id"], s["id"], 1.0, track_count(s), now, gid)
                        continue
                    try:
                        val = MB_EXTRACT[tag](r)
                    except (KeyError, IndexError):
                        continue
                    add(path, tag, val, r["id"], conf, track_count(r), now, gid)
        conn.commit()


# ---------- write ----------
def write_tags(conn, root, apply, backup):
    files = [r[0] for r in conn.execute("SELECT file_path FROM files_to_update ORDER BY 1")]
    total = conn.execute("SELECT COUNT(*) FROM tags_to_update").fetchone()[0]
    print(f"{len(files)} files / {total} tags to update")
    if not apply:
        for r in conn.execute("SELECT * FROM tags_to_update LIMIT 20"):
            print("  ", r)
        return print("Dry run. Use --apply to write.")
    ok = bad = 0
    for rel in files:
        rows = conn.execute("SELECT tag, current_value, wanted_value FROM tags_to_update WHERE file_path=?",
                            (rel,)).fetchall()
        values = {t: w for t, c, w in rows if w and w != c and t in MB_TAGS}
        if not values:
            continue
        p = root / rel
        fmt, db_mtime, db_size = conn.execute("SELECT fmt, mtime, size FROM files WHERE path=?", (rel,)).fetchone()
        if not p.exists() or (p.stat().st_mtime, p.stat().st_size) != (db_mtime, db_size):
            print(f"[SKIP] changed on disk, rescan first: {rel}")
            bad += 1
            continue
        try:
            if backup:
                dest = Path(backup) / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(p, dest)
            write_file(p, fmt, values)
            new = index_file(conn, root, rel, fmt)  # re-read = verification + db refresh
            if all(new.get(t) == v for t, v in values.items()):
                ok += 1
            else:
                print(f"[VERIFY FAILED] {rel}")
                bad += 1
        except Exception as e:
            print(f"[WRITE ERROR] {rel}: {e}")
            bad += 1
        conn.commit()
    print(f"Done: {ok} ok, {bad} failed/skipped")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.environ.get("LIBRARY_ROOT", "/music"))
    ap.add_argument("--db", default=os.environ.get("DB_PATH", "/data/tags.db"))
    ap.add_argument("--config", default=os.environ.get("TAGS_CONFIG", "/data/tags_config.json"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    for c in ("init", "rescan"):
        sub.add_parser(c)
    m = sub.add_parser("mb")
    m.add_argument("--refresh", action="store_true")
    w = sub.add_parser("write")
    w.add_argument("--apply", action="store_true")
    w.add_argument("--backup", help="copy each file here before writing")
    a = ap.parse_args()

    conn, root = connect(a.db), Path(a.root)
    conn.executescript(SCHEMA)
    migrate(conn)
    sync_config(conn, a.config)  # no-op on the very first init, init_db syncs after creating it
    {"init": lambda: init_db(conn, a.config),
     "rescan": lambda: rescan(conn, root),
     "mb": lambda: mb_query(conn, a.refresh),
     "write": lambda: write_tags(conn, root, a.apply, a.backup)}[a.cmd]()
    conn.commit()


if __name__ == "__main__":
    main()
