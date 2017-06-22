package me.charon.utils4j.kafka8;

import kafka.consumer.KafkaStream;
import me.charon.utils4j.kafka.Processor;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerWithProcessorP extends ConsumerWithProcessor {

    protected final CountDownLatch finished;

    protected final int num;

    protected final ExecutorService pool;

    public ConsumerWithProcessorP(String zk, String topic, String group, Processor processor, int num) {
        super(zk, topic, group, processor);
        this.num = num;
        this.finished = new CountDownLatch(num);
        this.pool = Executors.newFixedThreadPool(num);
    }

    @Override
    public void run() {
        List<KafkaStream<byte[], byte[]>> streams = getKafkaStreams(num);
        streams.stream().map(stream -> (Runnable) () -> {
            String name = Thread.currentThread().getName();
            try {
                log.info("[consume:%s] start to run...", name);
                consumeAndProcess(stream);
            } catch (Exception e) {
                log.error("consume exception", e);
            } finally {
                finished.countDown();
                log.info("[consume:%s] finish to run...", name);
            }
        }).forEach(pool::submit);
    }

    @Override
    public void shutdown() {
        running.set(false);

        while (finished.getCount() > 0) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
            log.info("waiting for threads to finish!");
        }

        pool.shutdown();
        try {
            while (!pool.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                log.info("waiting for pool to shutdown");
            }
        } catch (InterruptedException e) {

        }

        if (consumer != null) {
            consumer.commitOffsets(true);
            consumer.shutdown();
        }
        log.info("consumer is shut down!");
    }
}