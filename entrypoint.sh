#!/usr/bin/env bash
mongod --port 27017 --replSet rs0 --bind_ip localhost && sleep 5 && mongo --eval 'rs.initiate({   _id: "rs0",   members: [     {      _id: 0,      host: "localhost:27017"     }]})' & nginx & /app/kafka-backend & wait -n;
pkill -P $$
exit 1
