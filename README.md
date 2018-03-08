# kafka-spark-openshift-java

This project provides a simple Java application skeleton for including Apache
Kafka and Apache Spark with the [radanalytics.io](radanalytics.io) project. It
is designed to be deployed using a
[source-to-image](https://github.com/openshift/source-to-image) workflow onto
OpenShift.

Deploying OpenShift and Kafka are outside the scope of this readme, but for
a quick step up see the following:

* OpenShift https://docs.openshift.org/latest/getting_started/administrators.html
* Kafka http://strimzi.io/docs/0.1.0/
* radanalytics.io https://radanalytics.io/get-started

## Launching on OpenShift

```
oc new-app --template oshinko-java-spark-build-dc \
  -p APPLICATION_NAME=skeleton \
  -p GIT_URI=https://github.com/bones-brigade/kafka-spark-openshift-java \
  -p APP_MAIN_CLASS=org.bonesbrigade.skeletons.kafkasparkopenshift.App \
  -p SPARK_OPTIONS='--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.0 --conf spark.jars.ivy=/tmp/.ivy2' \
  -e KAFKA_BROKERS=kafka:9092 \
  -e KAFKA_TOPIC=sometopic
```
