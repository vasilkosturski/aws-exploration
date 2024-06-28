package frauddetection;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FraudDetectorTest {
    private StreamExecutionEnvironment env;
    private TestFraudAlertSink testSink;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder().build());

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        testSink = new TestFraudAlertSink();
    }

    @Test
    public void testFraudDetection() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        GeneratorFunction<Long, String> transactionGenerator = new SerializableGeneratorFunction<>(index -> {
            Instant startTime = Instant.parse("2023-05-15T12:00:00Z");
            Transaction[] transactions = new Transaction[]{
                    new Transaction("acc1", 50, startTime.plus(15, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc1", 75, startTime.plus(30, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc2", 20, startTime.plus(45, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc2", 100, startTime.plus(60, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc3", 65, startTime.plus(75, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc3", 120, startTime.plus(105, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc4", 30, startTime.plus(120, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc4", 110, startTime.plus(150, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc5", 5, startTime.plus(180, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc5", 1000, startTime.plus(195, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc6", 5, startTime.plus(240, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc6", 1500, startTime.plus(240, ChronoUnit.MINUTES).plus(30, ChronoUnit.SECONDS).toString()),
                    new Transaction("acc7", 45, startTime.plus(250, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc7", 85, startTime.plus(260, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc8", 9, startTime.plus(270, ChronoUnit.MINUTES).toString()),
                    new Transaction("acc8", 2000, startTime.plus(270, ChronoUnit.MINUTES).plus(30, ChronoUnit.SECONDS).toString())
            };
            if (index < transactions.length) {
                try {
                    return objectMapper.writeValueAsString(transactions[Math.toIntExact(index)]);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to serialize transaction", e);
                }
            } else {
                return null;
            }
        });

        long numberOfRecords = 16;

        DataGeneratorSource<String> dataGenSource = new DataGeneratorSource<>(
                transactionGenerator,
                numberOfRecords,
                TypeInformation.of(String.class)
        );

        FraudDetector fraudDetector = new FraudDetector(dataGenSource, testSink);
        fraudDetector.build(env);
        env.execute("Test Fraud Detection");

        List<String> results = testSink.getValues();

        assertEquals("Expected exactly two fraudulent transactions", 2, results.size());

        String expectedFraudulentAlertAcc6 = "{\"accountId\":\"acc6\"}";
        long countFraudAlertsAcc6 = results.stream().filter(alert -> alert.contains(expectedFraudulentAlertAcc6)).count();
        assertEquals("Expected exactly one fraudulent alert for account acc6", 1, countFraudAlertsAcc6);

        String expectedFraudulentAlertAcc8 = "{\"accountId\":\"acc8\"}";
        long countFraudAlertsAcc8 = results.stream().filter(alert -> alert.contains(expectedFraudulentAlertAcc8)).count();
        assertEquals("Expected exactly one fraudulent alert for account acc8", 1, countFraudAlertsAcc8);
    }

    @After
    public void tearDown() {
        TestFraudAlertSink.clear();
    }

    private static class SerializableGeneratorFunction<T, R> implements GeneratorFunction<T, R>, Serializable {
        private final GeneratorFunction<T, R> delegate;

        public SerializableGeneratorFunction(GeneratorFunction<T, R> delegate) {
            this.delegate = delegate;
        }

        @Override
        public R map(T value) throws Exception {
            return delegate.map(value);
        }
    }
}
