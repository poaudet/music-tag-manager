FROM python:3.12-slim
RUN pip install mutagen requests
COPY tagstage.py /app/tagstage.py
ENTRYPOINT ["python", "/app/tagstage.py"]
