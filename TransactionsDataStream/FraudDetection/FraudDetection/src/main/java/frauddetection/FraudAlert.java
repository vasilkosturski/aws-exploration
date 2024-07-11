package frauddetection;

public class FraudAlert {
    private String accountId;

    public String getAccountId() {
        return this.accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String toString() {
        return "Alert{accountId=" + this.accountId + '}';
    }
}
