FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY src ./src
EXPOSE 8080
# Raise the FD limit ceiling for high connection counts (compose sets ulimits too)
CMD ["node", "src/server.js"]
