package me.charon.utils4j.kafka;

/**
 * Created by a on 6/14/17.
 */
public class PrintProcessor implements Processor {
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
