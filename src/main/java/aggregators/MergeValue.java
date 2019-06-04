package aggregators;

import deserialization.Message;
import org.apache.spark.api.java.function.Function2;

/**
 * @author sfedosov on 5/9/19.
 */
public class MergeValue implements Function2<Object, Message, Object> {
    @Override
    public Object call(Object combiner, Message message) throws Exception {
        ((MessageAggregator) combiner).addMessage(message);
        return combiner;
    }
}
