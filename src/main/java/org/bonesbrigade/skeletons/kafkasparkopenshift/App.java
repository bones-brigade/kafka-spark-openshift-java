/* kafka-spark-java

This is a skeleton application for processing stream data from Apache
Kafka with Apache Spark. It will read messages on an input topic and
simply echo those message to the output topic.

This application uses Spark's _Structured Streaming_ interface, for
more information please see
https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
*/
package org.bonesbrigade.skeletons.kafkasparkopenshift;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;


public class App {
    public static void main(String[] args) throws Exception {
        String brokers = System.getenv("KAFKA_BROKERS");
        String intopic = System.getenv("KAFKA_IN_TOPIC");
        String outtopic = System.getenv("KAFKA_OUT_TOPIC");

        if (brokers == null) {
            System.out.println("KAFKA_BROKERS must be defined.");
            System.exit(1);
        }
        if (intopic == null) {
            System.out.println("KAFKA_IN_TOPIC must be defined.");
            System.exit(1);
        }

        if (outtopic == null) {
            System.out.println("KAFKA_OUT_TOPIC must be defined.");
            System.exit(1);
        }

        /* acquire a SparkSession object */
        SparkSession spark = SparkSession
            .builder()
            .appName("KafkaSparkOpenShiftJava")
            .getOrCreate();

        /* configure the operations to read the input topic */
        Dataset<Row> records = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("subscribe", intopic)
            .load()
            .select(functions.column("value").cast(DataTypes.StringType).alias("value"));
            /*
            * add your data operations here, the raw message is passed along as
            * the alias `value`.
            *
            * for example, to process the message as json and create the
            * corresponding objects you could do the following:
            *
            * .select(functions.from_json(functions.column('value'), msg_struct).alias('json'));
            *
            * the following operations would then access the object and its
            * properties using the name `json`.
            */

        /* configure the output stream */
        StreamingQuery writer = records
            .writeStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("topic", outtopic)
            .option("checkpointLocation", "/tmp")
            .start();

        /* begin processing the input and output topics */
        writer.awaitTermination();
    }
}
