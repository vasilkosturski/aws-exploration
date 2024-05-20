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
import static org.junit.Assert.assertTrue;

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
        env.execute("Test Fraud Detection");  // Ensure this is the only place `execute` is called for this environment.

        List<String> results = TestFraudAlertSink.getValues();
        assertEquals("Expected only one fraudulent alert", 1, results.size());
        String expectedJson = "{\"accountId\":\"acc123\"}";
        assertTrue("Expected alert not found in the output", results.contains(expectedJson));
    }

    @After
    public void tearDown() {
        TestFraudAlertSink.clear();
    }
}
