package me.charon.utils4j.kafka9;

import me.charon.utils4j.kafka.Processor;
import org.apache.kafka.common.errors.WakeupException;

import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerWithProcessorS extends ConsumerWithProcessor {

    private final AtomicBoolean finished;

    public ConsumerWithProcessorS(String brokers, String topic, String group, Processor processor) {
        super(brokers, topic, group, processor);
        this.finished = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        try {
            consumeAndProcess();
        } catch (WakeupException e1) {
            if (running.get())
                log.error("WakeupException when consuming", e1);
        } catch (Exception e2) {
            log.error("Exception when consuming", e2);
        } finally {
            log.info("consumer is closed!");
        }
    }

    @Override
    public void shutdown() {
        running.set(false);
        consumer.wakeup();

        while (!finished.get()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
            log.info("waiting for thread to finish!");
        }

        consumer.commitSync();
        consumer.close();
        log.info("consumer is shut down!");
    }
}