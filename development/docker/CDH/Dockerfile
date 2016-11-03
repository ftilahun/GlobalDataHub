#
#  Dockerfile for an CDH container
#

# build with 
# docker build -t "cdh:development" .

# run with:
# docker run --name=quickstart.cloudera --hostname=quickstart.cloudera --privileged=true  --restart=always -d -t -i -m 8G -p 80:80 -p 7180:7180 -p 8020:8020 -p 8042:8042 -p 8088:8088 -p 8888:8888 -p 10000:10000 -p 50070:50070 -p 50111:50111 -p 21050:21050 -p 50075:50075 -v /Users:/media/Users <imageID> /usr/bin/docker-quickstart

FROM cloudera/quickstart:5.7.0-0-beta
MAINTAINER Ciaran Kearney "c.kearney@kainos.com"

ADD servicesetup.py /root/servicesetup.py
RUN curl https://bootstrap.pypa.io/get-pip.py | python
RUN pip install cm-api
RUN /home/cloudera/cloudera-manager --express 

WORKDIR /root