package com.lacrimes.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ResponseTimeAgg {
    @JsonProperty("sum")
    private double sum;

    @JsonProperty("count")
    private long count;

    public ResponseTimeAgg() {}

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public ResponseTimeAgg add(double value) {
        this.sum += value;
        this.count++;
        return this;
    }

    public double avg() {
        return count == 0 ? 0.0 : sum / count;
    }
}
