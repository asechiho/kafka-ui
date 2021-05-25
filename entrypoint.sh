#!/usr/bin/env bash
/app/start_mongo_repl.sh & nginx & /app/kafka-backend & wait -n;
pkill -P $$
exit 1
