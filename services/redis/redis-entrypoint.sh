#!/bin/bash

#Here we ensure that pass is injected and real config is formed
set -e
sed "s|__REDIS_PASS__|${REDIS_PASS}|g" /usr/local/etc/redis/redis.conf.template > /usr/local/etc/redis/redis.conf
exec redis-server /usr/local/etc/redis/redis.conf