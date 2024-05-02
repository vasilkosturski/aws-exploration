package frauddetection;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.time.Instant;
import java.time.format.DateTimeParseException;

public class FraudDetectionFunction extends KeyedProcessFunction<String, Transaction, Alert> {
    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long SUSPICIOUS_TIME_DELTA = 60 * 1000;  // 1 minute in milliseconds

    private transient ValueState<Boolean> smallTransactionFlag;
    private transient ValueState<Long> lastTransactionEventTime;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> smallTransactionFlagDescriptor = new ValueStateDescriptor<>(
                "small-transaction-flag",
                Types.BOOLEAN);
        smallTransactionFlag = getRuntimeContext().getState(smallTransactionFlagDescriptor);

        ValueStateDescriptor<Long> eventTimeDescriptor = new ValueStateDescriptor<>(
                "last-transaction-event-time",
                Types.LONG);
        lastTransactionEventTime = getRuntimeContext().getState(eventTimeDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        long eventTime = parseEventTime(transaction.getEventTime());
        Long lastEventTime = lastTransactionEventTime.value();

        if (lastEventTime != null) {
            long timeDelta = eventTime - lastEventTime;

            if (smallTransactionFlag.value() != null && smallTransactionFlag.value() && transaction.getAmount() > LARGE_AMOUNT && timeDelta < SUSPICIOUS_TIME_DELTA) {
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            smallTransactionFlag.update(true);  // Flag this transaction as small
        } else {
            smallTransactionFlag.update(false); // Reset flag if the transaction is not small
        }

        lastTransactionEventTime.update(eventTime); // Always update the last transaction event time
    }

    private long parseEventTime(String eventTime) {
        try {
            return Instant.parse(eventTime).toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new RuntimeException("Failed to parse event time: " + eventTime, e);
        }
    }
}
