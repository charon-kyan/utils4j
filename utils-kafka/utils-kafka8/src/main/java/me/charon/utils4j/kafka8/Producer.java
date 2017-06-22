package me.charon.utils4j.kafka8;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by a on 6/13/17.
 */
public class Producer {

    private final String brokers;
    private final String topic;

    KafkaProducer<String, String> producer;

    public Producer(String brokers, String topic) {
        this.brokers = brokers;
        this.topic = topic;
        producer = new KafkaProducer<>(getProps(brokers));
    }

    private Properties getProps(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public void produce(String key, String msg) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, msg);
        producer.send(record);
    }

    public void close() {
        producer.close();
    }
}
