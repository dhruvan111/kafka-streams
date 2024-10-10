package org.example.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.example.data.RTSData;
import org.example.data.RTSStreamingPacketEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonDataUtils {
    private static final Logger log = LoggerFactory.getLogger(JsonDataUtils.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static KeyValue<String, RTSData> fromJson(String jsonString) throws JsonProcessingException {
        RTSStreamingPacketEvent event = mapper.readValue(jsonString, RTSStreamingPacketEvent.class);
        String streamId = event.getStreamId();  // Key

        RTSData data = new RTSData();
        data.setEventData(event.getEvents());   // Event data
        data.setFlag(event.isFlag());
        return KeyValue.pair(streamId, data);
    }

    public static String toJson(RTSData data) throws JsonProcessingException {
        return mapper.writeValueAsString(data);
    }
}
