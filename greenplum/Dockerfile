FROM ubuntu:18.04

RUN apt-get update && apt-get install -y openssh-server \
    && apt-get install -y software-properties-common \
    && apt-get install -y less vim sudo \
    && apt-get install -y locales iputils-ping \
    && apt-get clean \
    && add-apt-repository -y ppa:greenplum/db

RUN apt-get update && apt-get install -y greenplum-db-6 \
    && apt-get clean

WORKDIR /inst_scripts

# create gpadmin user
ADD gpadmin_user.sh .
RUN chmod 755 gpadmin_user.sh
RUN ./gpadmin_user.sh
RUN usermod -aG sudo gpadmin

#RUN ln -s /opt/greenplum-db-6.26.1 /opt/gpdb
#RUN chown -R gpadmin:gpadmin /opt/gpdb

# create master directory
RUN mkdir -p /var/lib/gpdb/data/gpmaster

# RUN mkdir /var/lib/gpdb/data/gpmaster/gpsne-1
# create data directories
RUN mkdir /var/lib/gpdb/data/gpdata1
RUN mkdir /var/lib/gpdb/data/gpdata2

# set locale
RUN locale-gen en_US.UTF-8  
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8 

WORKDIR /var/lib/gpdb/setup/

#REPLACE WITH "ADD hostlist ." to specify segment nodes
ADD multihost .
ADD singlehost .
ADD gpinitsys .
RUN chown -R gpadmin:gpadmin /var/lib/gpdb


ENV USER=gpadmin
ENV MASTER_DATA_DIRECTORY=/var/lib/gpdb/data/gpmaster/gpsne-1

# add the entrypoint script
ADD docker-entrypoint.sh /usr/local/bin/
ADD monitor_master.sh   .
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh
# add monitor script
RUN chmod +x monitor_master.sh
RUN chown -R gpadmin:gpadmin /var/lib/gpdb

#sshd must exist for gpdb monitor_master.sh
RUN echo 'gpadmin ALL=(ALL) NOPASSWD:/usr/sbin/sshd' >> /etc/sudoers


USER gpadmin

ENV GPHOME=/opt/greenplum-db-6.26.1
ENV PYTHONHOME=${GPHOME}/ext/python
ENV PATH=${PYTHONHOME}/bin:${PATH}
ENV LD_LIBRARY_PATH=${PYTHONHOME}/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
ENV PYTHONPATH=${GPHOME}/lib/python
ENV PATH=${GPHOME}/bin:${PATH}
ENV LD_LIBRARY_PATH=${GPHOME}/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
ENV OPENSSL_CONF=${GPHOME}/etc/openssl.cnf

ENV GP_NODE=master
ENV HOSTFILE=singlehost
#ENV HOSTFILE=multihost
####CHANGE THIS TO YOUR LOCAL SUBNET

VOLUME /var/lib/gpdb/data
ENTRYPOINT ["docker-entrypoint.sh"]
EXPOSE 5432

CMD ["./monitor_master.sh"]
