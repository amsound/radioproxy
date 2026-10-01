FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8010

RUN useradd -m -u 10001 -s /usr/sbin/nologin appuser
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY radioproxy ./radioproxy
# Logged at startup, to tell which build is running.
RUN date -u +"%Y-%m-%d %H:%M UTC" > /app/BUILD_DATE

EXPOSE 8010
USER appuser

HEALTHCHECK --interval=60s --timeout=5s --start-period=5s \
  CMD python -c "import os,urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ.get(\"PORT\",\"8010\")}/health', timeout=4)" || exit 1

CMD ["python", "-m", "radioproxy"]
