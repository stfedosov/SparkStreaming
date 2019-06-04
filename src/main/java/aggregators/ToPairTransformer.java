package aggregators;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * @author sfedosov on 5/10/19.
 */
public class ToPairTransformer implements PairFunction<Tuple2<String, Object>, String, Float> {

    @Override
    public Tuple2<String, Float> call(Tuple2<String, Object> tuple) throws Exception {
        long clicks = ((MessageAggregator) tuple._2()).getClicks();
        long views = ((MessageAggregator) tuple._2()).getViews();
        if (clicks + views >= 20L) {
            float ratio = (float) clicks / (clicks + views);
            if (((MessageAggregator) tuple._2()).seenCategories().size() > 5) {
                if (ratio > 0.6f && ratio != 1f) {
                    return new Tuple2<>(tuple._1(), ratio);
                }
            }
        }
        return new Tuple2<>("", 0.0f);
    }
}
