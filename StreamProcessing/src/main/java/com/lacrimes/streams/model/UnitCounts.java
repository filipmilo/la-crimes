package com.lacrimes.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UnitCounts {
    @JsonProperty("lapd")
    private long lapd;

    @JsonProperty("lafd")
    private long lafd;

    @JsonProperty("ems")
    private long ems;

    public UnitCounts() {}

    public long getLapd() {
        return lapd;
    }

    public void setLapd(long lapd) {
        this.lapd = lapd;
    }

    public long getLafd() {
        return lafd;
    }

    public void setLafd(long lafd) {
        this.lafd = lafd;
    }

    public long getEms() {
        return ems;
    }

    public void setEms(long ems) {
        this.ems = ems;
    }

    public UnitCounts add(String unit) {
        if (unit == null) return this;
        String upper = unit.toUpperCase();
        if (upper.startsWith("LAPD")) {
            this.lapd++;
        } else if (upper.startsWith("LAFD")) {
            this.lafd++;
        } else if (upper.startsWith("EMS")) {
            this.ems++;
        }
        return this;
    }

    public long total() {
        return lapd + lafd + ems;
    }
}
