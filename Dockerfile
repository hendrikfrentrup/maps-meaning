ARG BASE_CONTAINER=jupyter/pyspark-notebook
FROM $BASE_CONTAINER

USER root

# graphframes package available here: https://spark-packages.org/package/graphframes/graphframes
RUN cd /tmp && \
    wget --quiet http://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.7.0-spark2.4-s_2.11/graphframes-0.7.0-spark2.4-s_2.11.jar && \
    echo "5f88fa9cea8a4ed6144c2986cd2cfdfb graphframes-0.7.0-spark2.4-s_2.11.jar" | md5sum -c - && \
    cp graphframes-0.7.0-spark2.4-s_2.11.jar $SPARK_HOME/jars/ && \
    unzip -qq graphframes-0.7.0-spark2.4-s_2.11.jar && \
    cp -r graphframes $SPARK_HOME/python && \
    rm graphframes-0.7.0-spark2.4-s_2.11.jar && \
    rm -r graphframes

# ENV PYSPARK_SUBMIT_ARGS --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 pyspark-shell

USER $NB_UID