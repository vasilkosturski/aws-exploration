package frauddetection;

import java.util.Objects;

public class Transaction {
    private String accountId;
    private long timestamp; // is the timestamp needed?
    private double amount;
    private String eventTime;

    public Transaction() {
    }

    public Transaction(String accountId, long timestamp, double amount, String eventTime) {
        this.accountId = accountId;
        this.timestamp = timestamp;
        this.amount = amount;
        this.eventTime = eventTime;
    }

    // Getters and setters for all fields
    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return accountId.equals(that.accountId) &&
                timestamp == that.timestamp &&
                Double.compare(that.amount, amount) == 0 &&
                eventTime.equals(that.eventTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountId, timestamp, amount, eventTime);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "accountId='" + accountId + '\'' +
                ", timestamp=" + timestamp +
                ", amount=" + amount +
                ", eventTime='" + eventTime + '\'' +
                '}';
    }
}
