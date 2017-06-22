package me.charon.utils4j.kafka8;

import kafka.consumer.KafkaStream;
import me.charon.utils4j.kafka.Processor;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerWithProcessorS extends ConsumerWithProcessor {

    private final AtomicBoolean finished;

    public ConsumerWithProcessorS(String zk, String topic, String group, Processor processor) {
        super(zk, topic, group, processor);
        this.finished = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        try {
            log.info("[consume] start to run...");
            List<KafkaStream<byte[], byte[]>> streams = getKafkaStreams(1);
            consumeAndProcess(streams.get(0));
        } catch (Exception e) {
            log.error("consume exception", e);
        } finally {
            finished.set(true);
            log.info("[consume] finish to run...");
        }
    }

    @Override
    public void shutdown() {
        running.set(false);

        while (!finished.get()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
            log.info("waiting for thread to finish!");
        }

        if (consumer != null) {
            consumer.commitOffsets(true);
            consumer.shutdown();
        }

        log.info("consumer is shut down!");
    }
}