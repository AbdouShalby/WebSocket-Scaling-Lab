FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY src ./src
EXPOSE 8080
# File-descriptor limits are configured by Compose, not by this image.
CMD ["node", "src/server.js"]
