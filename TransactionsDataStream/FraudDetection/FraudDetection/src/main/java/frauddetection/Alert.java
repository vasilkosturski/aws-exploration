package frauddetection;

import java.util.Objects;

public class Alert {
    private long id;

    public Alert() {
    }

    public long getId() {
        return this.id;
    }

    public void setId(long id) {
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
        return "Alert{id=" + this.id + '}';
    }
}
