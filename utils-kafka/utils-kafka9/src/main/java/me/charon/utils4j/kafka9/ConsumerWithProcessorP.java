package me.charon.utils4j.kafka9;

import me.charon.utils4j.kafka.Processor;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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
        Arrays.asList(IntStream.range(0, num).toArray()).stream()
                .map(i -> (Runnable) () -> {
                    String name = Thread.currentThread().getName();
                    try {
                        log.info("[consume:%s] start to run...", name);
                        consumeAndProcess();
                    } catch (WakeupException e1) {
                        if (running.get())
                            log.error("WakeupException when consuming", e1);
                    } catch (Exception e2) {
                        log.error("Exception when consuming", e2);
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

        consumer.commitSync();
        consumer.close();
        log.info("consumer is shut down!");
    }
}