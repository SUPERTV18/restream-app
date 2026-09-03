FROM node:18

# تثبيت FFmpeg + Python + pip
RUN apt-get update && \
    apt-get install -y \
    ffmpeg \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# تثبيت yt-dlp
RUN pip3 install --no-cache-dir --break-system-packages -U yt-dlp

WORKDIR /app

COPY . .

RUN npm install

CMD ["node", "index.js"]
