#!/bin/sh

JAR_NAME="/data/semantic_crawl/anaconda3/bin/scrapyd"

SERVER_PID=`ps auxf | grep ${JAR_NAME} | grep -v "grep"| awk '{print $2}'`
echo "${JAR_NAME} pid is ${SERVER_PID}"
if [ -n $SERVER_PID ] 
then
  kill $SERVER_PID
  echo "$SERVER_PID is killed!"
fi
