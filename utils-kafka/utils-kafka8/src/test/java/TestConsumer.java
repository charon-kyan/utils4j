import me.charon.utils4j.kafka.Processor;
import me.charon.utils4j.kafka8.ConsumerWithProcessor;
import me.charon.utils4j.kafka8.ConsumerWithProcessorS;
import org.junit.Test;

/**
 * Created by a on 7/5/17.
 */
public class TestConsumer {

    @Test
    public void test1() {
        String zk = "10.103.35.1:2181,10.103.35.2:2181,10.103.35.10:2181/violet";
        String topic = "indata_str_opp_documents";
        String group = "tt_test";

        ConsumerWithProcessor consumer = new ConsumerWithProcessorS(zk, topic, group, new Processor() {
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
        });

        consumer.run();
    }
}
