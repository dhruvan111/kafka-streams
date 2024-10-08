package org.example.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.example.data.RTSData;

import static org.example.utils.JsonDataUtils.toJson;

@Slf4j
public class RTSDataSerializerProcessor implements Processor<String, RTSData, String, String> {
    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> context) {
        Processor.super.init(context);
        this.context = context;
    }

    @Override
    public void process(Record<String, RTSData> record) {
        String serializedData = null;
        try {
            serializedData = toJson(record.value());
        } catch (JsonProcessingException e) {
            log.error("Error serializing RTS data: {}", record.value());
        }

        if (serializedData != null) {
            Record<String, String> newRecord = new Record<>(record.key(), serializedData, record.timestamp(), record.headers());
            context.forward(newRecord);
        } else {
            log.error("serialized data should NOT be NULL: {}", record.value());
        }
    }
}
