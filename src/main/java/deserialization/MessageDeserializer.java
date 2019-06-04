package deserialization;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Date;
import java.util.Map;

/**
 * @author sfedosov on 4/29/19.
 */
public class MessageDeserializer implements Deserializer<Message> {

    private final Gson parser = new GsonBuilder().registerTypeAdapter(Date.class, new DateTypeAdapter()).create();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    @JsonCreator
    public Message deserialize(String topic, byte[] data) {
        String json = new String(data);
        Map objMap = parser.fromJson(json, Map.class);
        String payload = objMap.get("payload").toString().trim();
        if (payload.charAt(0) == '[') payload = payload.substring(1);
        Message message;
        try {
            message = parser.fromJson(payload, Message.class);
        } catch (JsonSyntaxException e) {
            System.out.println("Incorrect payload = " + payload);
            throw new RuntimeException(e);
        }
        return message;
    }

    @Override
    public void close() {

    }
}
