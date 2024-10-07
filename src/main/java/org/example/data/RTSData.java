package org.example.data;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class RTSData {
    private List<String> eventData = new ArrayList<>();

    public void combineEventData(RTSData rtsData) {
        this.eventData.addAll(rtsData.getEventData());
    }
}
