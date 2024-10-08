package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.data.RTSData;
import org.example.processors.AppendDataProcessor;
import org.example.processors.ExtractEventProcessor;
import org.example.processors.RTSDataSerializerProcessor;
import org.example.serde.RTSDataSerde;

import static org.example.config.ConfigProperties.getPropertiesForProcessors;

public class Main {
    private static final String INPUT_TOPIC = "input-streamID_005";
    private static final String OUTPUT_TOPIC = "output-streamID_005";

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
                .addProcessor("SERIALIZE", RTSDataSerializerProcessor::new, "APPEND")
                .addSink("Sink", OUTPUT_TOPIC, "SERIALIZE");
        return topology;
    }

    public static void main(String[] args) {
        KafkaStreams kafkaStreams = new KafkaStreams(createTopology(), getPropertiesForProcessors());
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
