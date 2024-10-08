package org.example.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.example.data.RTSData;
import org.example.data.RTSStreamingPacketEvent;

public class ExtractFromJson {
    public static KeyValue<String, RTSData> extractEvent(String jsonString) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        RTSStreamingPacketEvent event = mapper.readValue(jsonString, RTSStreamingPacketEvent.class);
        String streamId = event.getStreamId();  // Key

        RTSData data = new RTSData();
        data.setEventData(event.getEvents());   // Event data
        return KeyValue.pair(streamId, data);
    }
}
