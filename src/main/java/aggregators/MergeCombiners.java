package aggregators;

import org.apache.spark.api.java.function.Function2;

/**
 * @author sfedosov on 5/9/19.
 */
public class MergeCombiners implements Function2<Object, Object, Object> {
    @Override
    public Object call(Object combiner1, Object combiner2) throws Exception {
        ((MessageAggregator) combiner1).mergeAggregators((MessageAggregator) combiner2);
        return combiner1;
    }
}
