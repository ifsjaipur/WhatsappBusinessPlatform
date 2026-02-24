FROM python:3.12-slim

# System dependencies for aiortc (WebRTC) and audio processing
RUN apt-get update && apt-get install -y \
    curl \
    libavformat-dev \
    libavcodec-dev \
    libavdevice-dev \
    libavutil-dev \
    libavfilter-dev \
    libswscale-dev \
    libswresample-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create directories for persistent data (mount as volumes in production)
RUN mkdir -p recordings data

EXPOSE 7860

CMD ["python", "server.py", "--host", "0.0.0.0", "--port", "7860"]
