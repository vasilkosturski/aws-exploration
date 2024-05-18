package frauddetection;

import java.util.Objects;

public class Alert {
    private String accountId;

    public Alert() {
    }

    public String getAccountId() {
        return this.accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            Alert alert = (Alert)o;
            return this.accountId == alert.accountId;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(this.accountId);
    }

    public String toString() {
        return "Alert{accountId=" + this.accountId + '}';
    }
}
