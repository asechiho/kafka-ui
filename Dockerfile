FROM node:16 as web-builder
COPY webapp /app/webapp
RUN cd /app/webapp \
    && npm install \
    && npm run build

FROM golang:1.16.2 as backend-builder
COPY backend /app/backend
RUN cd /app/backend \
    && apt-get update \
    && apt-get -y install make gcc musl-dev librdkafka-dev \
    && go get -v -u go.mongodb.org/mongo-driver/mongo \
    && go get github.com/abice/go-enum \
    && make build

FROM nginx:1.19.8
COPY --from=web-builder /app/webapp/dist /etc/nginx/html
COPY --from=backend-builder /app/backend/kafka-backend /app/kafka-backend
COPY nginx.conf /etc/nginx/nginx.conf

RUN cd /tmp \
    && apt-get update \
    && apt-get install -y gnupg wget lsb-release \
    && export CODENAME=`lsb_release -cs` \
    && echo "deb https://download.rethinkdb.com/repository/debian-$CODENAME $CODENAME main" | tee /etc/apt/sources.list.d/rethinkdb.list \
    && echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/4.4 main" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list \
    && wget -qO- https://download.rethinkdb.com/repository/raw/pubkey.gpg | apt-key add - \
    && wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add - \
    && apt-get update \
    && apt-get install -y rethinkdb mongodb-org bash

COPY entrypoint.sh /app/entrypoint.sh
RUN mkdir -p /data/db \
    && echo -e 'systemLog:\n  destination: file\n  path: /mongo.log\n  logAppend: true\nstorage:\n  dbPath: /data/db\nnet:\n  bindIp: 127.0.0.1\nreplication:\n  replSetName: "rs0"' >> /mongod.conf

EXPOSE 80 9002
CMD ["/bin/bash", "/app/entrypoint.sh"]