package frauddetection;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class TestTransactionSource implements SourceFunction<String> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Instant startTime = Instant.parse("2023-05-15T12:00:00Z");

        Transaction[] transactions = new Transaction[] {
                // Normal transactions
                new Transaction("acc123", 50, startTime.plus(15, ChronoUnit.MINUTES).toString()),
                new Transaction("acc123", 75, startTime.plus(30, ChronoUnit.MINUTES).toString()),
                new Transaction("acc123", 20, startTime.plus(45, ChronoUnit.MINUTES).toString()),
                new Transaction("acc123", 100, startTime.plus(60, ChronoUnit.MINUTES).toString()),
                new Transaction("acc123", 65, startTime.plus(75, ChronoUnit.MINUTES).toString()),
                new Transaction("acc123", 120, startTime.plus(105, ChronoUnit.MINUTES).toString()),
                new Transaction("acc123", 30, startTime.plus(120, ChronoUnit.MINUTES).toString()),
                new Transaction("acc123", 110, startTime.plus(150, ChronoUnit.MINUTES).toString()),
                // Non-fraudulent transaction pair with a big time gap
                new Transaction("acc123", 5, startTime.plus(180, ChronoUnit.MINUTES).toString()),
                new Transaction("acc123", 1000, startTime.plus(195, ChronoUnit.MINUTES).toString()),
                // Fraudulent transaction pair with a small time gap
                new Transaction("acc123", 5, startTime.plus(240, ChronoUnit.MINUTES).toString()),
                new Transaction("acc123", 1500, startTime.plus(240, ChronoUnit.MINUTES)
                        .plus(30, ChronoUnit.SECONDS).toString())
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
