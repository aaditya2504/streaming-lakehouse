#!/bin/sh
while true; do
  echo "[`date`] running maintenance..."
  python maintenance/maintain.py
  sleep 1800   # every 30 min
done