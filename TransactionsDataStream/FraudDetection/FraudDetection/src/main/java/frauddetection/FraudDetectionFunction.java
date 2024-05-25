package frauddetection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.format.DateTimeParseException;

public class FraudDetectionFunction extends KeyedProcessFunction<String, Transaction, FraudAlert> {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionFunction.class);

    private static final double SMALL_AMOUNT = 10.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long SUSPICIOUS_TIME_DELTA = 60 * 1000;

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
    public void processElement(Transaction transaction, Context context, Collector<FraudAlert> collector) throws Exception {
        long eventTime = parseEventTime(transaction.getEventTime());
        Long lastEventTime = lastTransactionEventTime.value();
        LOG.info("Processing transaction: {}", transaction);

        if (lastEventTime != null) {
            long timeDelta = eventTime - lastEventTime;
            LOG.info("Time delta: {}", timeDelta);

            boolean isFraud = smallTransactionFlag.value() != null && smallTransactionFlag.value() &&
                    transaction.getAmount() > LARGE_AMOUNT && timeDelta < SUSPICIOUS_TIME_DELTA;
            LOG.info("Fraud detected: {}", isFraud);

            if (isFraud) {
                FraudAlert alert = new FraudAlert();
                alert.setAccountId(transaction.getAccountId());
                collector.collect(alert);
                LOG.info("Alert generated: {}", alert);
            }
        }

        smallTransactionFlag.update(transaction.getAmount() < SMALL_AMOUNT);

        lastTransactionEventTime.update(eventTime);
    }

    private long parseEventTime(String eventTime) {
        try {
            // Parses the date-time string which should be in ISO-8601 format directly to an Instant
            return Instant.parse(eventTime).toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new RuntimeException("Failed to parse event time: " + eventTime, e);
        }
    }
}
