package com.lacrimes.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CallerInfo {
    @JsonProperty("phone_number")
    private String phoneNumber;

    @JsonProperty("is_anonymous")
    private boolean isAnonymous;

    public CallerInfo() {}

    public CallerInfo(String phoneNumber, boolean isAnonymous) {
        this.phoneNumber = phoneNumber;
        this.isAnonymous = isAnonymous;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public boolean isAnonymous() {
        return isAnonymous;
    }

    public void setAnonymous(boolean anonymous) {
        isAnonymous = anonymous;
    }

    @Override
    public String toString() {
        return "CallerInfo{" +
                "phoneNumber='" + phoneNumber + '\'' +
                ", isAnonymous=" + isAnonymous +
                '}';
    }
}
