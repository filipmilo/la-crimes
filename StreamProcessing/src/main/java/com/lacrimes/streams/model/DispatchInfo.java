package com.lacrimes.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class DispatchInfo {
    @JsonProperty("units_dispatched")
    private List<String> unitsDispatched;

    @JsonProperty("response_time_minutes")
    private double responseTimeMinutes;

    @JsonProperty("is_dispatched")
    private boolean isDispatched;

    public DispatchInfo() {}

    public DispatchInfo(List<String> unitsDispatched, double responseTimeMinutes, boolean isDispatched) {
        this.unitsDispatched = unitsDispatched;
        this.responseTimeMinutes = responseTimeMinutes;
        this.isDispatched = isDispatched;
    }

    public List<String> getUnitsDispatched() {
        return unitsDispatched;
    }

    public void setUnitsDispatched(List<String> unitsDispatched) {
        this.unitsDispatched = unitsDispatched;
    }

    public double getResponseTimeMinutes() {
        return responseTimeMinutes;
    }

    public void setResponseTimeMinutes(double responseTimeMinutes) {
        this.responseTimeMinutes = responseTimeMinutes;
    }

    public boolean isDispatched() {
        return isDispatched;
    }

    public void setDispatched(boolean dispatched) {
        isDispatched = dispatched;
    }

    @Override
    public String toString() {
        return "DispatchInfo{" +
                "unitsDispatched=" + unitsDispatched +
                ", responseTimeMinutes=" + responseTimeMinutes +
                ", isDispatched=" + isDispatched +
                '}';
    }
}
