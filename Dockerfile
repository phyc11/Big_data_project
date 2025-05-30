FROM apache/airflow:2.6.0-python3.9

USER root

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ant && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$PATH:$JAVA_HOME/bin"



USER airflow
