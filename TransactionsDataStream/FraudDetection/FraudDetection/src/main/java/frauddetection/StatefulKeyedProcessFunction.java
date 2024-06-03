package frauddetection;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class StatefulKeyedProcessFunction extends KeyedProcessFunction<Long, Long, Long> {
    private transient ValueState<Long> state;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("state", Long.class);
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
        Long currentState = state.value() != null ? state.value() : 0L;
        currentState += value;
        state.update(currentState);
        out.collect(currentState);
    }
}
