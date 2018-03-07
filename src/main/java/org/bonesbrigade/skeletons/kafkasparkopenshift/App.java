package org.bonesbrigade.skeletons.kafkasparkopenshift;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class App {
    public static void main(String[] args) throws Exception {
        String brokers = System.getenv("KAFKA_BROKERS");
        String topic = System.getenv("KAFKA_TOPIC");

        if (brokers == null) {
            System.out.println("KAFKA_BROKERS must be defined.");
            System.exit(1);
        }
        if (topic == null) {
            System.out.println("KAFKA_TOPIC must be defined.");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("KafkaSparkOpenShiftApp");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

        Set<String> topicSet = new HashSet<String>();
        topicSet.add(topic);
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("metadata.brokers.list", brokers);

        JavaInputDStream<ConsumerRecord<Object, Object>> messages = KafkaUtils.createDirectStream(
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topicSet, kafkaParams));

        JavaDStream<String> lines = messages.map(l -> l.value().toString());
        lines.print();

        jssc.start();
        System.out.println("waiting for input");
        jssc.awaitTermination();
    }
}
