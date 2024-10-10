package org.example.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.data.RTSData;

public class AppendDataProcessor implements Processor<String, RTSData, String, RTSData> {
    private ProcessorContext<String, RTSData> context;
    private KeyValueStore<String, RTSData> dataStore;
    private int threshold;

    @Override
    public void init(ProcessorContext<String, RTSData> context) {
        Processor.super.init(context);
        this.context = context;
        this.dataStore = context.getStateStore("data-store");
        this.threshold = 10;
    }

    @Override
    public void process(Record<String, RTSData> record) {
        String streamId = record.key();

        RTSData previousData = dataStore.get(streamId);
        if (previousData == null || previousData.getEventData().size() >= threshold) {
            dataStore.put(streamId, record.value());
        } else {
            previousData.combineEventData(record.value());
            dataStore.put(streamId, previousData);
        }

        RTSData updatedRTSData = dataStore.get(streamId);
        if (record.value().isFlag()) {
            updatedRTSData.setFlag(true);
        }

        if (updatedRTSData.getEventData().size() >= threshold || updatedRTSData.isFlag()) {
            Record<String, RTSData> currentRecord = new Record<>(streamId, updatedRTSData, record.timestamp(), record.headers());
            context.forward(currentRecord);
        }
    }
}
