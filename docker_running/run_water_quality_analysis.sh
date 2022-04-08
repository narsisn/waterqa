#!/bin/bash
### Maintainer: Narges Norouzi - This script runs the task from groningen university

if [ "$1" == "stop" ]; then 
	echo "Stopping containers..."
	docker-compose down
elif [ "$1" == "start" ]; then
	echo "Starting containers ..."
	docker-compose up -d
	[[ "$?" -eq 0 ]] && echo "Containers are up and running!"
	sleep 4
	echo "Please copy the following URL to your browser and run .ipynb files from jupyter interface"
	IP=$(ip route get 8.8.8.8 | awk '{gsub(".*src",""); print $1; exit}')
	[[ $IP == "" ]] && IP="127.0.0.1"
	URL=$(docker logs spark-master 2>&1 | grep -Eo '?token=.*' | head -1)
	echo "http://$IP:8888/?$URL"
fi
