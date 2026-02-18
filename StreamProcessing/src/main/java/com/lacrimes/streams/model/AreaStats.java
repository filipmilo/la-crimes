package com.lacrimes.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AreaStats {
    @JsonProperty("area_risk_level")
    private String areaRiskLevel;

    @JsonProperty("area_performance_tier")
    private String areaPerformanceTier;

    @JsonProperty("crime_density_category")
    private String crimeDensityCategory;

    @JsonProperty("resolution_rate")
    private double resolutionRate;

    @JsonProperty("crime_density_percentile")
    private double crimeDensityPercentile;

    public AreaStats() {}

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

    public double getCrimeDensityPercentile() {
        return crimeDensityPercentile;
    }

    public void setCrimeDensityPercentile(double crimeDensityPercentile) {
        this.crimeDensityPercentile = crimeDensityPercentile;
    }
}
