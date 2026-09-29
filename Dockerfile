FROM python:3.11-slim

WORKDIR /usr/src/app

COPY . .

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    git \
    libffi-dev \
    libxml2-dev \
    libxslt1-dev \
    libgl1 \
    libglib2.0-0 \
    poppler-utils \
    libreoffice \
    fonts-nanum \
    fonts-noto-cjk \
    vim \
    curl \
    && fc-cache -f \
    && rm -rf /var/lib/apt/lists/*

# 인용 뷰어가 HWPX 를 쪽 이미지로 그릴 때 쓴다(app/services/rendition.py). codex 와 같은 버전.
ARG RHWP_VERSION=0.8.4
ARG RHWP_SHA256=d2f015447147a840b3a587e8e9bedd75973fb2e0a60eac37b08cad9e34cdac54
ADD https://github.com/edwardkim/rhwp/releases/download/v${RHWP_VERSION}/rhwp-v${RHWP_VERSION}-linux-x86_64.tar.gz /tmp/rhwp.tar.gz
RUN echo "${RHWP_SHA256}  /tmp/rhwp.tar.gz" | sha256sum -c - \
    && tar xzf /tmp/rhwp.tar.gz -C /tmp \
    && install -m 0755 /tmp/rhwp/rhwp /usr/local/bin/rhwp \
    && rm -rf /tmp/rhwp /tmp/rhwp.tar.gz \
    && rhwp capabilities > /dev/null

ENV MAX_JOBS=1
ENV MALLOC_ARENA_MAX=2

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80

CMD ["python", "main.py"]
