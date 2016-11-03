#
#  Dockerfile for an attunity container
#

# build with 
# docker build -t "attunity:development" .

# run with:
# docker run --name=attunity --hostname=attunity --privileged=true  --restart=always --link <id of cloudera container> -d -t -i -p 3552:3552  -v /Users:/media/Users <image id>

FROM centos:latest
MAINTAINER Ciaran Kearney "ciaran@antipathy.org"

ADD odbcinst.ini  /root/odbcinst.ini
ADD repctl.sh /root/repctl.sh
ADD AttunityReplicate_Express_Linux_X64.rpm /root/AttunityReplicate_Express_Linux_X64.rpm

RUN yum -y install nano
RUN yum -y install unixODBC 
RUN yum -y install mysql
RUN yum -y install mysql-connector-odbc

RUN rpm -ivh /root/AttunityReplicate_Express_Linux_X64.rpm
RUN rm /opt/attunity/replicate/bin/repctl.sh
RUN cp /root/repctl.sh /opt/attunity/replicate/bin/repctl.sh
RUN rm /etc/odbcinst.ini
RUN cp /root/odbcinst.ini /etc/odbcinst.ini
RUN /opt/attunity/replicate/bin/repctl.sh SETSERVERPASSWORD admin
RUN /opt/attunity/replicate/bin/arep.ctl stop
RUN /opt/attunity/replicate/bin/arep.ctl start
RUN echo '/opt/attunity/replicate/bin/arep.ctl start' >> /etc/rc.d/rc.local

WORKDIR /root