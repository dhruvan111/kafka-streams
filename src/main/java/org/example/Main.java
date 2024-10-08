package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.data.RTSData;
import org.example.processors.AppendDataProcessor;
import org.example.processors.ExtractEventProcessor;
import org.example.processors.RTSDataSerializerProcessor;
import org.example.serde.RTSDataSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.example.config.ConfigProperties.getPropertiesForProcessors;
import static org.example.utils.JsonDataUtils.fromJson;

public class Main {
    private static final String INPUT_TOPIC = "input-streamID_005";
    private static final String OUTPUT_TOPIC = "output-streamID_005";
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final long THRESHOLD = 2;

    private static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> sourceStream = streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, RTSData> transformedStream = sourceStream.map((key, jsonString) -> {
            try {
                logger.info("Prefix: Extracting data: {}", jsonString);
                return fromJson(jsonString);
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
                    if (packet1.getEventData().size() >= 2) {
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


    private static StoreBuilder<KeyValueStore<String, RTSData>> buildStore() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("data-store"),
                Serdes.String(),
                new RTSDataSerde()
        );
    }

    private static Topology createTopology() {
        Topology topology = new Topology();
        topology.addSource("SOURCE", INPUT_TOPIC)
                .addProcessor("EXTRACTION", ExtractEventProcessor::new, "SOURCE")
                .addProcessor("APPEND", AppendDataProcessor::new, "EXTRACTION")
                .addStateStore(buildStore(), "APPEND")
                .addProcessor("SER", RTSDataSerializerProcessor::new, "APPEND")
                .addSink("Sink", OUTPUT_TOPIC, "SER");
        return topology;
    }

    public static void main(String[] args) {
        KafkaStreams kafkaStreams = new KafkaStreams(createTopology(), getPropertiesForProcessors());
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
