package com.lacrimes.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DispatchStats {
    @JsonProperty("total")
    private long total;

    @JsonProperty("failed")
    private long failed;

    public DispatchStats() {}

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getFailed() {
        return failed;
    }

    public void setFailed(long failed) {
        this.failed = failed;
    }

    public DispatchStats add(boolean isDispatched) {
        this.total++;
        if (!isDispatched) {
            this.failed++;
        }
        return this;
    }

    public double failureRate() {
        return total == 0 ? 0.0 : (double) failed / total;
    }
}
