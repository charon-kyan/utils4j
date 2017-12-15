package me.charon.utils4j.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by a on 6/13/17.
 */
public interface Processor {


    void process(String record);

    void init();

    void destroy();
}
