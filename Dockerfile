FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

COPY app ./app
COPY public ./public
COPY verify ./verify
COPY tests ./tests

# Build check at image build time: everything must byte-compile.
RUN python -m compileall -q app verify tests

CMD ["python", "-m", "app.server"]
