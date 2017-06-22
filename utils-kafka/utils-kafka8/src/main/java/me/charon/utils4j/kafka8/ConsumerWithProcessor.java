package me.charon.utils4j.kafka8;

import com.google.common.collect.ImmutableMap;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import me.charon.utils4j.kafka.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by a on 6/13/17.
 */
abstract public class ConsumerWithProcessor {

    protected static final Logger log = LoggerFactory.getLogger(ConsumerWithProcessor.class);
    protected static final Logger consumeLog = LoggerFactory.getLogger("consume");

    protected final AtomicBoolean running = new AtomicBoolean(true);

    protected final String topic;
    protected final Processor processor;

    protected ConsumerConnector consumer;

    public ConsumerWithProcessor(String zk, String topic, String group, Processor processor) {
        this.topic = topic;
        this.processor = processor;
        this.consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(getProps(zk, group)));
    }

    protected Properties getProps(String zk, String group) {
        Properties props = new Properties();
        props.put("group.id", group);
        props.put("zookeeper.connect", zk);
        props.put("exclude.internal.topics", "false");
        props.put("auto.commit.interval.ms", "1000");
        return props;
    }

    protected void consumeAndProcess(KafkaStream<byte[], byte[]> stream) {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (running.get()) {
            if (it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> msgAndMd = it.next();
                int partition = msgAndMd.partition();
                long offset = msgAndMd.offset();
                String message = new String(msgAndMd.message());

                // consumeLog.debug("[consume] [{}-{}] msg: {}", partition, offset, message);
                consumeLog.debug("[consume] {}:{}", partition, offset);
                try {
                    processor.process(message);
                } catch (Exception e) {
                    log.error("process exception:", message, e);
                }
            }
        }
    }

    protected List<KafkaStream<byte[], byte[]>> getKafkaStreams(int num) {
        return consumer.createMessageStreams(ImmutableMap.of(topic, num)).get(topic);
    }

    public abstract void run();

    public abstract void shutdown();

}
