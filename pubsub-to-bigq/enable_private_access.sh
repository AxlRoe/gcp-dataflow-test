#!/bin/bash

if [[ x$1 == "x" ]]; then
  echo "missing reserved range name"
  exit 1
fi

gcloud beta compute addresses create $1 --global --prefix-length=24 --network=default --purpose=vpc_peering
