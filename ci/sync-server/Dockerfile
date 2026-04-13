FROM node:20-alpine

WORKDIR /app

COPY worker/dist/ ./worker/dist/
COPY package.json yarn.lock ./

RUN yarn install --production && mkdir -p /app/data

ENV DB_SYNC_PORT=8787
ENV DB_SYNC_DATA_DIR=/app/data/db-sync
ENV DB_SYNC_LOG_LEVEL=info

VOLUME ["/app/data"]
EXPOSE 8787

CMD ["node", "worker/dist/node-adapter.js"]
