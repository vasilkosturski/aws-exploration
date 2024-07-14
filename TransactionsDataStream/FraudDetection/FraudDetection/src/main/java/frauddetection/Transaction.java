package frauddetection;

public class Transaction {
    private String accountId;
    private double amount;
    private String eventTime;

    public Transaction() {
    }

    public Transaction(String accountId, double amount, String eventTime) {
        this.accountId = accountId;
        this.amount = amount;
        this.eventTime = eventTime;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
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
    public String toString() {
        return "Transaction{" +
                "accountId='" + accountId + '\'' +
                ", amount=" + amount +
                ", eventTime='" + eventTime + '\'' +
                '}';
    }
}
