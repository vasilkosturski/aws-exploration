package frauddetection;

import java.util.Objects;

public class Transaction {
    private long accountId;
    private long timestamp;
    private double amount;

    public Transaction() {
    }

    public Transaction(long accountId, long timestamp, double amount) {
        this.accountId = accountId;
        this.timestamp = timestamp;
        this.amount = amount;
    }

    public long getAccountId() {
        return this.accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getAmount() {
        return this.amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            Transaction that = (Transaction)o;
            return this.accountId == that.accountId && this.timestamp == that.timestamp && Double.compare(that.amount, this.amount) == 0;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(this.accountId, this.timestamp, this.amount);
    }

    public String toString() {
        return "Transaction{accountId=" + this.accountId + ", timestamp=" + this.timestamp + ", amount=" + this.amount + '}';
    }
}
