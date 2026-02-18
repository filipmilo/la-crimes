package com.lacrimes.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Call911 {
    @JsonProperty("call_id")
    private String callId;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("call_type")
    private String callType;

    @JsonProperty("incident_type")
    private String incidentType;

    @JsonProperty("priority")
    private int priority;

    @JsonProperty("location")
    private Location location;

    @JsonProperty("caller_info")
    private CallerInfo callerInfo;

    @JsonProperty("dispatch_info")
    private DispatchInfo dispatchInfo;

    @JsonProperty("status")
    private String status;

    @JsonProperty("area_risk_level")
    private String areaRiskLevel;

    @JsonProperty("area_performance_tier")
    private String areaPerformanceTier;

    @JsonProperty("crime_density_category")
    private String crimeDensityCategory;

    @JsonProperty("resolution_rate")
    private double resolutionRate;

    public Call911() {}

    public String getCallId() {
        return callId;
    }

    public void setCallId(String callId) {
        this.callId = callId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getCallType() {
        return callType;
    }

    public void setCallType(String callType) {
        this.callType = callType;
    }

    public String getIncidentType() {
        return incidentType;
    }

    public void setIncidentType(String incidentType) {
        this.incidentType = incidentType;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    public CallerInfo getCallerInfo() {
        return callerInfo;
    }

    public void setCallerInfo(CallerInfo callerInfo) {
        this.callerInfo = callerInfo;
    }

    public DispatchInfo getDispatchInfo() {
        return dispatchInfo;
    }

    public void setDispatchInfo(DispatchInfo dispatchInfo) {
        this.dispatchInfo = dispatchInfo;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getAreaRiskLevel() {
        return areaRiskLevel;
    }

    public void setAreaRiskLevel(String areaRiskLevel) {
        this.areaRiskLevel = areaRiskLevel;
    }

    public String getAreaPerformanceTier() {
        return areaPerformanceTier;
    }

    public void setAreaPerformanceTier(String areaPerformanceTier) {
        this.areaPerformanceTier = areaPerformanceTier;
    }

    public String getCrimeDensityCategory() {
        return crimeDensityCategory;
    }

    public void setCrimeDensityCategory(String crimeDensityCategory) {
        this.crimeDensityCategory = crimeDensityCategory;
    }

    public double getResolutionRate() {
        return resolutionRate;
    }

    public void setResolutionRate(double resolutionRate) {
        this.resolutionRate = resolutionRate;
    }

    @Override
    public String toString() {
        return "Call911{" +
                "callId='" + callId + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", callType='" + callType + '\'' +
                ", incidentType='" + incidentType + '\'' +
                ", priority=" + priority +
                ", location=" + location +
                ", callerInfo=" + callerInfo +
                ", dispatchInfo=" + dispatchInfo +
                ", status='" + status + '\'' +
                '}';
    }
}
