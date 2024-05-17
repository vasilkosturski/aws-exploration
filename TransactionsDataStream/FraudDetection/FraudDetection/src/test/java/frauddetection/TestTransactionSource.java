package frauddetection;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;

public class TestTransactionSource implements SourceFunction<String> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Transaction[] transactions = new Transaction[] {
                // Small transaction
                new Transaction("12345", 0.99, "2023-05-15T12:00:00Z"),
                // Large transaction within 60 seconds
                new Transaction("12345", 501.00, "2023-05-15T12:00:50Z"),
                // Normal transaction
                new Transaction("67890", 20.0, "2023-05-15T12:10:00Z")
        };

        for (Transaction transaction : transactions) {
            try {
                String json = objectMapper.writeValueAsString(transaction);
                ctx.collect(json);
            } catch (IOException e) {
                // Handle exception from Jackson serialization
                System.err.println("Failed to serialize transaction: " + e.getMessage());
            }
        }
    }

    @Override
    public void cancel() {}
}
