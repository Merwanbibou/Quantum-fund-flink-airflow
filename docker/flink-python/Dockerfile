FROM apache/flink:1.17.1-python3.11

# Install PyFlink + Kafka client
USER root
RUN pip install --no-cache-dir apache-flink==1.17.1 kafka-python==2.0.2
USER flink
