package frauddetection;

import java.util.Objects;

public class Alert {
    private String id;

    public Alert() {
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            Alert alert = (Alert)o;
            return this.id == alert.id;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(this.id);
    }

    public String toString() {
        return "Alert{accountId=" + this.id + '}';
    }
}
