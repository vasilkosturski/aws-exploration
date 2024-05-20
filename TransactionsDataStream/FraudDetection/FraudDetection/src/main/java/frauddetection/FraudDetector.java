package frauddetection;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

public class FraudDetector {
    private final SourceFunction<String> source;
    private final Sink<String> sink;
    private static final ObjectMapper mapper = new ObjectMapper();

    public FraudDetector(SourceFunction<String> source, Sink<String> sink) {
        this.source = source;
        this.sink = sink;
    }

    public void build(StreamExecutionEnvironment env) {
        DataStream<Transaction> transactions = env.addSource(source)
                .map((MapFunction<String, Transaction>) value -> mapper.readValue(value, Transaction.class));

        DataStream<FraudAlert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetectionFunction())
                .name("fraud-detector");

        DataStream<String> alertStrings = alerts.map((MapFunction<FraudAlert, String>) alert ->
                mapper.writeValueAsString(alert));

        alertStrings.sinkTo(sink);
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
        FlinkKinesisConsumer<String> consumer = new FlinkKinesisConsumer<>("TransactionsInputStream", new SimpleStringSchema(), inputProperties);

        Properties outputProperties = new Properties();
        outputProperties.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        KinesisStreamsSink<String> sink = KinesisStreamsSink.<String>builder()
                .setKinesisClientProperties(outputProperties)
                .setSerializationSchema(new SimpleStringSchema())
                .setStreamName("TransactionsOutputStream")
                .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                .build();

        new FraudDetector(consumer, sink).build(env);
        env.execute("Fraud Detection");
    }
}
