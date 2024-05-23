FROM python:3.8-slim

WORKDIR /usr/src/app

COPY . .

ENV MAX_JOBS=1
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80


CMD ["uvicorn", "memento-service:app", "--host", "0.0.0.0", "--port", "8005"]