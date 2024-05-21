package frauddetection;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FraudDetectorTest {
    private StreamExecutionEnvironment env;
    private TestTransactionSource testSource;
    private TestFraudAlertSink testSink;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder().build());

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        testSource = new TestTransactionSource();
        testSink = new TestFraudAlertSink();
    }

    @Test
    public void testFraudDetection() throws Exception {
        FraudDetector fraudDetector = new FraudDetector(testSource, testSink);
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
}
