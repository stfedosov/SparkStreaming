package aggregators;

import deserialization.Message;
import org.apache.spark.api.java.function.Function;

/**
 * @author sfedosov on 5/9/19.
 */
public class CreateCombiner implements Function<Message, Object> {
    @Override
    public Object call(Message message) throws Exception {
        MessageAggregator aggregator = new MessageAggregator();
        aggregator.addMessage(message);
        return aggregator;
    }
}
