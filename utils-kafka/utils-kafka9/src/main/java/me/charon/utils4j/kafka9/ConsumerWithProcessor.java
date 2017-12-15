package me.charon.utils4j.kafka9;

import me.charon.utils4j.kafka.Processor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by a on 6/13/17.
 */
public abstract class ConsumerWithProcessor {

    protected static final Logger log = LoggerFactory.getLogger(ConsumerWithProcessor.class);
    protected static final Logger consumeLog = LoggerFactory.getLogger("consume");

    protected final AtomicBoolean running = new AtomicBoolean(true);

    protected final String topic;
    protected final Processor processor;

    protected KafkaConsumer<String, String> consumer;


    public ConsumerWithProcessor(String brokers, String topic, String group, Processor processor) {
        consumer = new KafkaConsumer<>(getProps(brokers, group));
        this.topic = topic;
        this.processor = processor;
    }

    protected Properties getProps(String brokers, String group) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", group);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    protected void consumeAndProcess() {
        log.info("consumer starts to run!");
        consumer.subscribe(Collections.singletonList(this.topic));
        while (running.get()) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            Iterator<ConsumerRecord<String, String>> it = consumerRecords.iterator();
            while (it.hasNext()) {
                ConsumerRecord<String, String> record = it.next();
                String value = record.value();
                // consumeLog.debug("consume {}:{} for {}: {}", record.partition(), record.offset(), record.topic(), value);
                consumeLog.debug("consume {}:{} for {}", record.partition(), record.offset(), record.topic());
                try {
                    processor.process(value);
                } catch (Exception e) {
                    log.error("process exception: " + value, e);
                }
            }
            // consumeLog.debug("%s records are processed".format(consumerRecords.count))
        }
    }

    public abstract void run();

    public abstract void shutdown();
}
