FROM python:3.10

WORKDIR $HOME/serving
COPY . $HOME/serving

RUN apt-get update && apt-get install -y --no-install-recommends\
    build-essential \
    make \
    gcc \
    g++ \
    postgresql \
    && rm -rf /var/lib/apt/lists/*

# set environment variables
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

RUN pip install --upgrade pip && pip install -r requirements.txt
