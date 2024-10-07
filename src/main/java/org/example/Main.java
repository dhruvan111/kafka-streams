package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.example.data.RTSData;
import org.example.data.RTSStreamingPacketEvent;
import org.example.serde.RTSDataSerde;

import java.util.Properties;

public class Main {
    private static final String INPUT_TOPIC = "input-streamID_001";
    private static final String OUTPUT_TOPIC = "output-streamID_001";
    private static final long THRESOLD = 4;

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> sourceStream = streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, RTSData> transformedStream = sourceStream.map((key, jsonString) -> {
            try {
                return extractEvent(jsonString);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Transformation goes wrong, from String to RTS", e);
            }
        });

        KStream<String, RTSData> combinedStream = transformedStream
                .groupByKey()
                .reduce((packet1, packet2) -> {
                    packet1.combineEventData(packet2);
                    return packet1;
                }).toStream();

        combinedStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), new RTSDataSerde()));
        return streamsBuilder.build();
    }

    private static KeyValue<String, RTSData> extractEvent(String jsonString) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        RTSStreamingPacketEvent event = mapper.readValue(jsonString, RTSStreamingPacketEvent.class);
        String streamId = event.getStreamId();  // Key

        RTSData data = new RTSData();
        data.setEventData(event.getEvents());   // Event data
        return KeyValue.pair(streamId, data);
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rts-stream-topology");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RTSDataSerde.class.getName());
        return props;
    }

    public static void main(String[] args) {
        KafkaStreams kafkaStreams = new KafkaStreams(buildTopology(), getProperties());
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
