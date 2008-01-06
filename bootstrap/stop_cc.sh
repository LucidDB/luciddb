#!/bin/bash
pid=`jps | grep CruiseControl | awk '{print $1}'`
if [ "$pid" > 0 ]; then
	echo "Kill $pid"
	kill $pid
	sleep 1 
	jps | grep CruiseControl
fi
