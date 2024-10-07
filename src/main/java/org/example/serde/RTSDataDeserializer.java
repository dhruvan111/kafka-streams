package org.example.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.example.data.RTSData;

import java.io.IOException;
import java.util.Map;

public class RTSDataDeserializer implements Deserializer<RTSData> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Configuration logic
    }

    @Override
    public RTSData deserialize(String topic, byte[] data) {
        try {
            if (data == null || data.length == 0) {
                return null;
            }
            return objectMapper.readValue(data, RTSData.class);
        } catch (IOException e) {
            throw new RuntimeException("Error deserializing byte[] to RTSData", e);
        }
    }

    @Override
    public void close() {
        // Close resources
    }
}
