package org.example.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.example.data.RTSData;
import org.example.utils.ExtractFromJson;

@Slf4j
public class ExtractEventProcessor implements Processor<String, String, String, RTSData> {
    private ProcessorContext<String, RTSData> context;

    @Override
    public void init(ProcessorContext<String, RTSData> context) {
        Processor.super.init(context);
        this.context = context;
    }

    @Override
    public void process(Record<String, String> record) {
        String jsonString = record.value();
        try {
            KeyValue<String, RTSData> extractedData = ExtractFromJson.extractEvent(jsonString);
            Record<String, RTSData> currentRecord = new Record<>(extractedData.key, extractedData.value, record.timestamp(), record.headers());
            context.forward(currentRecord);
        } catch (JsonProcessingException e) {
            log.info("Json conversion error for data: {}", jsonString);
        }

    }
}
