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
    private CollectingSink collectingSink;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        testSource = new TestTransactionSource();
        collectingSink = new CollectingSink();
    }

    @Test
    public void testFraudDetection() throws Exception {
        new FraudDetector(testSource, collectingSink).build(env);

        env.execute("Test Fraud Detection");

        List<String> results = CollectingSink.getValues();
        assertEquals("Expected only one fraudulent alert", 1, results.size());

        String expectedJson = "{\"accountId\":\"12345\", \"alert\":\"High transaction amount detected\"}";
        assertTrue("Expected alert not found in the output", results.contains(expectedJson));
    }

    @After
    public void tearDown() {
        CollectingSink.clear();
    }
}
