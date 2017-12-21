#!/bin/bash

pid=`ps -ef | grep 'ssh -f -N -L localhost:3306' | grep -v grep | awk '{print $2}'`

if [[ "" == "$pid" ]]; then
	echo "Setting up secure tunnel to remote MYSQL database on 127.0.0.1:3306..."
	ssh -f -N -L localhost:3306:localhost:3306 uberlisk@173.255.241.200
	pid=`ps -ef | grep 'ssh -f -N -L localhost:3306' | grep -v grep | awk '{print $2}'`
fi

echo "MySQL tunnel established at PID: $pid"

echo "Starting Jupyter Notebook..."
jupyter notebook

echo "Shutdown issued, closing tunnel process at PID: ${pid}..."
kill $pid
