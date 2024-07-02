package frauddetection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import software.amazon.msk.auth.iam.IAMClientCallbackHandler;

public class FraudDetector {
    private final Source<String, ?, ?> source;
    private final Sink<String> sink;
    private static final ObjectMapper mapper = new ObjectMapper();

    public FraudDetector(Source<String, ?, ?> source, Sink<String> sink) {
        this.source = source;
        this.sink = sink;
    }

    public void build(StreamExecutionEnvironment env) {
        DataStream<Transaction> transactions = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
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

        // Read broker information from environment variable
        String kafkaBrokers = System.getenv("KAFKA_BROKERS");

        if (kafkaBrokers == null) {
            throw new RuntimeException("Environment variable KAFKA_BROKERS must be set");
        }

        String inputTopic = "transactions-input";
        String outputTopic = "transactions-output";

        // Kafka source configuration with IAM authentication
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics(inputTopic)
                .setGroupId("fraud-detection-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("security.protocol", "SASL_SSL")
                .setProperty("sasl.mechanism", "AWS_MSK_IAM")
                .setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
                .setProperty("sasl.client.callback.handler.class", IAMClientCallbackHandler.class.getName())
                .build();

        // Kafka sink configuration with IAM authentication
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setProperty("security.protocol", "SASL_SSL")
                .setProperty("sasl.mechanism", "AWS_MSK_IAM")
                .setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
                .setProperty("sasl.client.callback.handler.class", IAMClientCallbackHandler.class.getName())
                .build();

        new FraudDetector(source, sink).build(env);
        env.execute("Fraud Detection");
    }
}
