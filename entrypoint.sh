#!/usr/bin/env bash
mongod --replSet rs0 --bind_ip localhost & nginx & /app/kafka-backend & wait -n;
pkill -P $$
exit 1
