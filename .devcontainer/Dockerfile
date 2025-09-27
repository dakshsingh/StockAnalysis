# Base image: Python with dev tools
FROM mcr.microsoft.com/devcontainers/python:3.12

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential cmake git wget sudo vim && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://github.com/ta-lib/ta-lib/releases/download/v0.6.4/ta-lib-0.6.4-src.tar.gz && \
    tar -xzf ta-lib-0.6.4-src.tar.gz && \
    cd ta-lib-0.6.4/ && \
    ./configure --prefix=/usr && \
    make && \
    make install
RUN rm -R ta-lib-0.6.4 ta-lib-0.6.4-src.tar.gz

WORKDIR /workspace

