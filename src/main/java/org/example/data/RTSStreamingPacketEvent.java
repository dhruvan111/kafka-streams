package org.example.data;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class RTSStreamingPacketEvent {
    private String streamId;
    private List<String> events = new ArrayList<>();
}
