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
            // Normal transactions for different accounts
            new Transaction("acc1", 50, startTime.plus(15, ChronoUnit.MINUTES).toString()),
            new Transaction("acc1", 75, startTime.plus(30, ChronoUnit.MINUTES).toString()),
            new Transaction("acc2", 20, startTime.plus(45, ChronoUnit.MINUTES).toString()),
            new Transaction("acc2", 100, startTime.plus(60, ChronoUnit.MINUTES).toString()),
            new Transaction("acc3", 65, startTime.plus(75, ChronoUnit.MINUTES).toString()),
            new Transaction("acc3", 120, startTime.plus(105, ChronoUnit.MINUTES).toString()),
            new Transaction("acc4", 30, startTime.plus(120, ChronoUnit.MINUTES).toString()),
            new Transaction("acc4", 110, startTime.plus(150, ChronoUnit.MINUTES).toString()),

            // Non-fraudulent transaction pair with a big time gap for account 5
            new Transaction("acc5", 5, startTime.plus(180, ChronoUnit.MINUTES).toString()),
            new Transaction("acc5", 1000, startTime.plus(195, ChronoUnit.MINUTES).toString()),

            // Fraudulent transaction pair with a small time gap for account 6
            new Transaction("acc6", 5, startTime.plus(240, ChronoUnit.MINUTES).toString()),
            new Transaction("acc6", 1500, startTime.plus(240, ChronoUnit.MINUTES).plus(30, ChronoUnit.SECONDS).toString()),

            // Additional normal transactions for more diversity
            new Transaction("acc7", 45, startTime.plus(250, ChronoUnit.MINUTES).toString()),
            new Transaction("acc7", 85, startTime.plus(260, ChronoUnit.MINUTES).toString()),

            // Additional fraudulent transactions
            new Transaction("acc8", 9, startTime.plus(270, ChronoUnit.MINUTES).toString()),
            new Transaction("acc8", 2000, startTime.plus(270, ChronoUnit.MINUTES).plus(30, ChronoUnit.SECONDS).toString()),
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
