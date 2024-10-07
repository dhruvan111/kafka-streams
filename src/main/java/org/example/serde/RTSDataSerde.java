package org.example.serde;

import org.apache.kafka.common.serialization.Serdes;
import org.example.data.RTSData;

public class RTSDataSerde extends Serdes.WrapperSerde<RTSData> {
    public RTSDataSerde() {
        super(new RTSDataSerializer(), new RTSDataDeserializer());
    }
}
