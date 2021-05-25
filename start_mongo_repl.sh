#!/bin/bash
start_replication() {
  sleep 5
  # shellcheck disable=SC2006
  for i in {1..5}
  do
    repl_status=$(mongo --eval 'rs.initiate({"_id": "rs0", "members": [{"_id": 0, "host": "127.0.0.1"}]});' | grep ok | cut -d ":" -f 2 | cut -d " " -f 2)
    echo "repl status by cut: $repl_status"
    if [ "$repl_status" != "1" ] ; then
      if [[ "$i" -eq 5 ]] ; then
        exit 1
      fi

      sleep 5
      continue
    fi

    break
  done
  echo "replication set success"
}

start_replication & mongod --logpath /app/logfile --logappend --replSet rs0 --bind_ip localhost