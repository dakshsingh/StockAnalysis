# Base image: Python with dev tools
FROM mcr.microsoft.com/devcontainers/python:3.11

# Set working directory
WORKDIR /workspace

# Install TA-Lib dependencies and build
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    libtool \
    libffi-dev \
    && wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz \
    && tar -xvzf ta-lib-0.4.0-src.tar.gz \
    && cd ta-lib && ./configure --prefix=/usr && make && make install && cd .. \
    && rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# Install Python dependencies from requirements.txt (assumes file in repo root)
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
