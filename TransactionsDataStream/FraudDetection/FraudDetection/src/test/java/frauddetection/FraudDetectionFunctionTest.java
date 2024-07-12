package frauddetection;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FraudDetectionFunctionTest {
    private KeyedOneInputStreamOperatorTestHarness<String, Transaction, FraudAlert> testHarness;
    private FraudDetectionFunction fraudDetectionFunction;

    @Before
    public void setupTestHarness() throws Exception {
        fraudDetectionFunction = new FraudDetectionFunction();
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(fraudDetectionFunction),
                Transaction::getAccountId,
                Types.STRING
        );
        testHarness.open();
    }

    @Test
    public void testFraudulentTransactions() throws Exception {
        testHarness.processElement(new StreamRecord<>(new Transaction("account1", 5.0, "2023-06-04T10:00:00Z"), 100L));
        testHarness.processElement(new StreamRecord<>(new Transaction("account1", 600.0, "2023-06-04T10:00:30Z"), 200L));

        FraudAlert expectedAlert = new FraudAlert();
        expectedAlert.setAccountId("account1");

        FraudAlert actual = extractSingleOutput(testHarness);
        assertEquals(expectedAlert.getAccountId(), actual.getAccountId());
    }

    @Test
    public void testNonFraudulentTransactionsTimeDelta() throws Exception {
        testHarness.processElement(new StreamRecord<>(new Transaction("account1", 5.0, "2023-06-04T10:00:00Z"), 100L));
        testHarness.processElement(new StreamRecord<>(new Transaction("account1", 600.0, "2023-06-04T10:02:00Z"), 200L));

        List<FraudAlert> actual = extractOutput(testHarness);
        assertTrue(actual.isEmpty());
    }

    @Test
    public void testNonFraudulentTransactionsAmount() throws Exception {
        testHarness.processElement(new StreamRecord<>(new Transaction("account1", 5.0, "2023-06-04T10:00:00Z"), 100L));
        testHarness.processElement(new StreamRecord<>(new Transaction("account1", 15.0, "2023-06-04T10:00:30Z"), 200L));

        List<FraudAlert> actual = extractOutput(testHarness);
        assertTrue(actual.isEmpty());
    }

    @Test
    public void testDifferentAccounts() throws Exception {
        testHarness.processElement(new StreamRecord<>(new Transaction("account1", 5.0, "2023-06-04T10:00:00Z"), 100L));
        testHarness.processElement(new StreamRecord<>(new Transaction("account2", 600.0, "2023-06-04T10:00:30Z"), 200L));

        List<FraudAlert> actual = extractOutput(testHarness);
        assertTrue(actual.isEmpty());
    }

    private List<FraudAlert> extractOutput(KeyedOneInputStreamOperatorTestHarness<String, Transaction, FraudAlert> harness) {
        return harness.getOutput().stream()
                .map(record -> ((StreamRecord<FraudAlert>) record).getValue())
                .collect(Collectors.toList());
    }

    private FraudAlert extractSingleOutput(KeyedOneInputStreamOperatorTestHarness<String, Transaction, FraudAlert> harness) {
        return extractOutput(harness).get(0);
    }
}
