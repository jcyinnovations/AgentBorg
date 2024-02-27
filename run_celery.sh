#!/bin/bash

celery worker -A proj -b amqp://admin:password@localhost:5672/ --loglevel=INFO --concurrency=5 -n worker1@%h --logfile=celery_transactions.log