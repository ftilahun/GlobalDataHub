#!/bin/sh

echo ############################################
echo # Creating Attunity docker image
echo ############################################
cd Attunity
docker build -t "enstar/attunity:latest" .
echo ############################################
echo # Creating CDH docker image
echo ############################################
cd ../CDH
docker build -t "enstar/cdh:5.7.0" .

echo ############################################
echo # Running CDH docker image
echo ############################################
docker run --name=quickstart.cloudera --hostname=quickstart.cloudera --privileged=true  --restart=always -d -t -i -m 8G -p 80:80 -p 7180:7180 -p 8020:8020 -p 8042:8042 -p 8088:8088 -p 8888:8888 -p 10000:10000 -p 50070:50070 -p 50111:50111 -p 21050:21050 -p 50075:50075 -v /Users:/media/Users enstar/cdh:5.7.0 /usr/bin/docker-quickstart
echo ############################################
echo # Running Attunity docker image
echo ############################################
docker run --name=attunity --hostname=attunity --privileged=true  --restart=always --link quickstart.cloudera -d -t -i -p 3552:3552  -v /Users:/media/Users enstar/attunity:latest

echo ############################################
echo # Done
echo # use: docker attach attunity
echo # or 
echo # use: docker attach quickstart.cloudera
echo # to open a terminal on either container.
echo ############################################