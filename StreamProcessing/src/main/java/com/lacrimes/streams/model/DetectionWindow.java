package com.lacrimes.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DetectionWindow {
    @JsonProperty("area_name")
    private String areaName;

    @JsonProperty("window_start")
    private long windowStart;

    @JsonProperty("window_end")
    private long windowEnd;

    @JsonProperty("count")
    private long count;

    public DetectionWindow() {}

    public DetectionWindow(String areaName, long windowStart, long windowEnd, long count) {
        this.areaName = areaName;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getAreaName() {
        return areaName;
    }

    public void setAreaName(String areaName) {
        this.areaName = areaName;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
