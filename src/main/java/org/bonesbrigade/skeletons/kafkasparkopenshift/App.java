package org.bonesbrigade.skeletons.kafkasparkopenshift;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

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

        SparkConf conf = new SparkConf().setAppName("KafkaSparkOpenShiftApp");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

        Set<String> topicSet = new HashSet<String>();
        topicSet.add(intopic);
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "stream1");

        JavaInputDStream<ConsumerRecord<Object, Object>> messages = KafkaUtils.createDirectStream(
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topicSet, kafkaParams));

        JavaDStream<String> lines = messages.map(l -> l.value().toString());

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        lines.foreachRDD(r -> r.foreach(new VoidFunction<String>() {
            public void call(String s) {
                Producer<String, String> producer = new KafkaProducer<>(props);
                producer.send(new ProducerRecord<String, String>(outtopic, s));
            }
        }));

        jssc.start();
        System.out.println("waiting for input");
        jssc.awaitTermination();
    }
}
