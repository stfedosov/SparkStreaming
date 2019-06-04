import aggregators.CreateCombiner;
import aggregators.MergeCombiners;
import aggregators.MergeValue;
import aggregators.ToPairTransformer;
import deserialization.Message;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import static utils.CommonUtils.BATCH_DURATION_IN_SECONDS;
import static utils.CommonUtils.WINDOW_LENGTH_IN_MINUTES;
import static utils.CommonUtils.createMessageJavaDStream;
import static utils.CommonUtils.createJavaSparkContext;
import static utils.CommonUtils.initializeIgniteCache;
import static utils.CommonUtils.print;

/**
 * @author sfedosov on 4/24/19
 */
public class AnalyzeDStream {

    private static final Function<Tuple2<String, Float>, Boolean> nonEmptyResults = v1 -> !v1._1().isEmpty();

    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext sc = createJavaSparkContext();
        JavaStreamingContext streamingContext =
                new JavaStreamingContext(sc, Durations.seconds(BATCH_DURATION_IN_SECONDS));
        JavaDStream<Message> dStream = createMessageJavaDStream(streamingContext);
        JavaIgniteRDD<String, Float> igniteCache = initializeIgniteCache(sc);

        JavaPairDStream<String, Message> pairDStream =
                dStream.mapToPair(message -> new Tuple2<>(message.getIp(), message))
                        .window(Durations.minutes(WINDOW_LENGTH_IN_MINUTES));

        JavaPairDStream<String, Float> pairs = pairDStream
                .combineByKey(new CreateCombiner(), new MergeValue(), new MergeCombiners(), new HashPartitioner(1))
                .mapToPair(new ToPairTransformer())
                .filter(nonEmptyResults);

        pairs.foreachRDD((VoidFunction<JavaPairRDD<String, Float>>) igniteCache::savePairs);

        streamingContext.start();

        print(igniteCache);

        streamingContext.awaitTermination();
        streamingContext.stop();
    }

}
