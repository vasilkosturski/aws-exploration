package frauddetection;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CollectingSink implements Sink<String> {
    private static final List<String> values = Collections.synchronizedList(new ArrayList<>());

    @Override
    public SinkWriter<String> createWriter(Sink.InitContext context) {
        return new CollectingSinkWriter(values);
    }

    public static List<String> getValues() {
        return values;
    }

    public static void clear() {
        values.clear();
    }

    private static class CollectingSinkWriter implements SinkWriter<String> {
        private final List<String> values;

        public CollectingSinkWriter(List<String> values) {
            this.values = values;
        }

        @Override
        public void write(String element, Context context) {
            values.add(element);
        }

        @Override
        public void flush(boolean b) {
        }

        @Override
        public void close() {
        }
    }
}
