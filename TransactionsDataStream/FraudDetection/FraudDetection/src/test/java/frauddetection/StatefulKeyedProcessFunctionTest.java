package frauddetection;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class StatefulKeyedProcessFunctionTest {
    private KeyedOneInputStreamOperatorTestHarness<Long, Long, Long> testHarness;
    private StatefulKeyedProcessFunction statefulKeyedProcessFunction;

    @Before
    public void setupTestHarness() throws Exception {
        statefulKeyedProcessFunction = new StatefulKeyedProcessFunction();
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(statefulKeyedProcessFunction),
                value -> 5L,  // Key selector
                Types.LONG
        );
        testHarness.open();
    }

    @Test
    public void testingStatefulKeyedProcessFunction() throws Exception {
        // Process elements with the same key
        testHarness.processElement(new StreamRecord<>(2L, 100L));
        testHarness.processElement(new StreamRecord<>(3L, 200L));

        assertEquals(Arrays.asList(2L, 5L), extractOutput(testHarness));
    }

    private List<Long> extractOutput(KeyedOneInputStreamOperatorTestHarness<Long, Long, Long> harness) {
        return harness.getOutput().stream()
                .map(record -> ((StreamRecord<Long>) record).getValue())
                .collect(Collectors.toList());
    }
}
