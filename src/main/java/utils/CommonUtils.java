package utils;

import deserialization.Message;
import deserialization.MessageDeserializer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * @author sfedosov on 5/21/19.
 */
public class CommonUtils {

    private static final int TTL_IN_MINUTES = 1;
    public static final int WINDOW_LENGTH_IN_MINUTES = 10;
    private static final String TOPIC = "file-topic-1";
    private static final String KAFKA_HOST = "127.0.0.1:9092";
    public static final int BATCH_DURATION_IN_SECONDS = 10;
    private static final String ERROR = "ERROR";
    private static final String PATH = "config/example-ignite.xml";

    public static JavaDStream<Message> createMessageJavaDStream(JavaStreamingContext streamingContext) {
        return KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, Message>
                        Subscribe(Collections.singletonList(TOPIC), getKafkaParams())
        )
                .map(ConsumerRecord::value);
    }

    public static JavaSparkContext createJavaSparkContext() {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("MainApplication");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel(ERROR);
        return sc;
    }

    public static JavaIgniteRDD<String, Float> initializeIgniteCache(JavaSparkContext sc) {
        JavaIgniteContext<String, Float> igniteContext = new JavaIgniteContext<>(sc, PATH, false);
        return igniteContext.fromCache(getCacheConfig());
    }

    private static Map<String, Object> getKafkaParams() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "1");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return kafkaParams;
    }

    public static void print(JavaIgniteRDD<String, Float> igniteCache) throws InterruptedException {
        TimeUnit.SECONDS.sleep(2 * BATCH_DURATION_IN_SECONDS);
        System.out.println("Ignite cache entries: ");
        igniteCache.foreach(tuple2 -> System.out.println("ip = " + tuple2._1() +
                ", clicks/views ratio = " + String.format("%.2f", tuple2._2())));
        System.out.println("Total cache size: " + igniteCache.collect().size());
    }

    private static CacheConfiguration<String, Float> getCacheConfig() {
        CacheConfiguration<String, Float> cfg = new CacheConfiguration<>();
        cfg.setName("myCache");
        cfg.setExpiryPolicyFactory(() -> new CreatedExpiryPolicy(new Duration(MINUTES, TTL_IN_MINUTES)));
        return cfg;
    }
}
