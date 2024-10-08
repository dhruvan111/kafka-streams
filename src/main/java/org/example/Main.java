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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Main {
    private static final String INPUT_TOPIC = "input-streamID_004";
    private static final String OUTPUT_TOPIC = "output-streamID_004";
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final long THRESHOLD = 2;

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> sourceStream = streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, RTSData> transformedStream = sourceStream.map((key, jsonString) -> {
            try {
                logger.info("Prefix: Extracting data: {}", jsonString);
                return extractEvent(jsonString);
            } catch (JsonProcessingException e) {
                logger.error("JSON converting error conversion: {}", jsonString);
                return KeyValue.pair("ERROR", new RTSData());
            }
        });

        KStream<String, RTSData> filteredStream = transformedStream
                .groupByKey()
                .reduce((packet1, packet2) -> {
                    double startTime = System.currentTimeMillis();
                    logger.info("currentSize: {} & previousStoredSize: {}", packet2.getEventData().size(), packet1.getEventData().size());
                    if (packet1.getEventData().size() >= THRESHOLD) {
                        return packet2;
                    }
                    packet1.combineEventData(packet2);
                    double endTime = System.currentTimeMillis();
                    logger.info("Time taken to append: {}", (endTime - startTime));
                    return packet1;
                }).filter((key, value) -> {
                    double startTime = System.currentTimeMillis();
                    logger.info("Key: {}", key);
                    logger.info("ValueSize: {}", value.getEventData().size());
                    for (String event : value.getEventData()) {
                        logger.info("Data: {}", event);
                    }
                    double endTime = System.currentTimeMillis();
                    logger.info("Time taken to filter: {}", (endTime - startTime));
                    return value.getEventData().size() >= THRESHOLD;
                }).toStream();

        filteredStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), new RTSDataSerde()));
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
