package me.charon.utils4j.kafka9.test;

import me.charon.utils4j.kafka.Processor;
import me.charon.utils4j.kafka9.ConsumerWithProcessor;
import me.charon.utils4j.kafka9.ConsumerWithProcessorS;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by a on 6/13/17.
 */
public class ConsumerTest {

    @Test
    public void test() {
        Processor processor;
        {
            processor = new PrintProcessor();
        }

        ConsumerWithProcessor consumer;
        {
            String brokers = "10.103.17.101:9092,10.103.17.102:9092,10.103.17.103:9092,10.103.17.104:9092,10.103.17.105:9092,10.103.17.106:9092";
            String topic = "opp_stats";
            String group = "opp_stats4s_writer_20170729";
            consumer = new ConsumerWithProcessorS(brokers, topic, group, processor) {

                @Override
                protected Properties getProps(String zk, String group) {
                    Properties props = super.getProps(zk, group);
                    props.put("max.partition.fetch.bytes", "10485760");
                    return props;
                }
            };
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.shutdown();
            processor.destroy();
        }));

        consumer.run();
    }

    public static class PrintProcessor implements Processor {

        @Override
        public void process(String record) {
            System.out.println(record);
        }

        @Override
        public void init() {

        }

        @Override
        public void destroy() {
        }
    }
}
