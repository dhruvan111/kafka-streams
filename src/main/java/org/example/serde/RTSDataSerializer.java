package org.example.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.example.data.RTSData;

import java.util.Map;

public class RTSDataSerializer implements Serializer<RTSData> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Configuration logic
    }

    @Override
    public byte[] serialize(String topic, RTSData data) {
        try {
            if (data == null) {
                return null;
            }
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing RTSData to byte[]", e);
        }
    }

    @Override
    public void close() {
        // Close resources
    }
}
