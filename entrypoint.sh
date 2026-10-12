#!/bin/sh
socat tcp6-listen:9094,ipv6only=0,fork,reuseaddr tcp-connect:10.31.81.1:9094 &
exec python3 main.py "$@"