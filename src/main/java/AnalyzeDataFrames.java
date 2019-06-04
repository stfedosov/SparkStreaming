import com.google.common.collect.Lists;
import deserialization.Message;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import static deserialization.Message.TYPE.click;
import static deserialization.Message.TYPE.view;
import static utils.CommonUtils.BATCH_DURATION_IN_SECONDS;
import static utils.CommonUtils.WINDOW_LENGTH_IN_MINUTES;
import static utils.CommonUtils.createMessageJavaDStream;
import static utils.CommonUtils.createJavaSparkContext;
import static utils.CommonUtils.initializeIgniteCache;
import static utils.CommonUtils.print;

/**
 * @author sfedosov on 4/24/19.
 */
public class AnalyzeDataFrames {

    private static final String IP = "ip", TYPE = "type", CATEGORY_ID = "category_id", UNIX_TIME = "unix_time";
    private static final String RATIO = "ratio";
    private static final String VIEW = view.name();
    private static final String CLICK = click.name();
    private static final String COUNT_DISTINCT = "countDistinctCategoryId";

    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext sc = createJavaSparkContext();
        JavaStreamingContext streamingContext =
                new JavaStreamingContext(sc, Durations.seconds(BATCH_DURATION_IN_SECONDS));
        JavaDStream<Message> dStream = createMessageJavaDStream(streamingContext);
        JavaIgniteRDD<String, Float> igniteCache = initializeIgniteCache(sc);

        dStream.foreachRDD(rdd -> {
            JavaRDD<Row> rowRDD = rdd.map(msg ->
                    RowFactory.create(msg.getUnixTime(), msg.getCategoryId(), msg.getIp(), msg.getType().name()));
            Dataset<Row> df = SparkSession
                    .builder()
                    .config(rdd.context().getConf())
                    .getOrCreate()
                    .createDataFrame(rowRDD, getSchema());

            JavaRDD<Row> rowJavaRDD = df
                    .select(df.col(IP), df.col(TYPE), df.col(UNIX_TIME), functions.size(
                            functions.collect_set(CATEGORY_ID).over(Window.partitionBy(IP))).alias(COUNT_DISTINCT))
                    .groupBy(functions.window(df.col(UNIX_TIME), WINDOW_LENGTH_IN_MINUTES + " minutes"),
                            df.col(IP),
                            new Column(COUNT_DISTINCT))
                    .pivot(TYPE, Lists.newArrayList(VIEW, CLICK))
                    .agg(functions.count(TYPE))
                    .filter(new Column(VIEW).plus(new Column(CLICK)).$greater(20)
                            .and(new Column(COUNT_DISTINCT).$greater(5)))
                    .withColumn(RATIO, new Column(CLICK).divide(new Column(CLICK).plus(new Column(VIEW))))
                    .filter(new Column(RATIO).$greater(0.6f)
                            .and(new Column(RATIO).notEqual(1.0f)))
                    .toJavaRDD();

            igniteCache.savePairs(
                    rowJavaRDD.mapToPair(row -> new Tuple2<>(row.getString(1), (float) row.getDouble(5))));

        });

        streamingContext.start();

        print(igniteCache);

        streamingContext.awaitTermination();
        streamingContext.stop();
    }

    @NotNull
    private static StructType getSchema() {
        return new StructType(new StructField[]{
                new StructField(UNIX_TIME, DataTypes.TimestampType, false, Metadata.empty()),
                new StructField(CATEGORY_ID, DataTypes.IntegerType, false, Metadata.empty()),
                new StructField(IP, DataTypes.StringType, false, Metadata.empty()),
                new StructField(TYPE, DataTypes.StringType, false, Metadata.empty()),
        });
    }
}
