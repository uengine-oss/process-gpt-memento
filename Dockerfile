FROM python:3.11-slim

WORKDIR /usr/src/app

COPY . .

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libffi-dev \
    libxml2-dev \
    libxslt1-dev \
    libgl1 \
    libglib2.0-0 \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

ENV MAX_JOBS=1

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80

CMD ["uvicorn", "memento-service:app", "--host", "0.0.0.0", "--port", "8005"]
