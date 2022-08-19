#!/bin/sh
# Guinicor startup shell script

gunicorn --worker-class eventlet -w 1 --threads 10 -b 0.0.0.0:8000 app:app