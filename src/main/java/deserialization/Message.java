package deserialization;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * @author sfedosov on 4/30/19.
 */
public class Message implements Serializable {

    public Message() {
    }

    public enum TYPE {
        click, view
    }

    @SerializedName("type")
    private TYPE type;

    @SerializedName("category_id")
    private int categoryId;

    @SerializedName("ip")
    private String ip;

    @SerializedName("unix_time")
    private Timestamp unixTime;

    public TYPE getType() {
        return type;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public String getIp() {
        return ip;
    }

    public Timestamp getUnixTime() {
        return unixTime;
    }

    public void setType(TYPE type) {
        this.type = type;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setUnixTime(Timestamp unixTime) {
        this.unixTime = unixTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return unixTime == message.unixTime &&
                Objects.equals(ip, message.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, unixTime);
    }

    @Override
    public String toString() {
        return "deserialization.Message{" +
                "type='" + type.name() + '\'' +
                ", categoryId=" + categoryId +
                ", ip='" + ip + '\'' +
                ", unixTime=" + unixTime +
                '}';
    }

}
