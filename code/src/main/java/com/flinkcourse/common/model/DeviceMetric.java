package com.flinkcourse.common.model;

import java.io.Serializable;

/**
 * IoT 设备指标（第13章 设备监控实战）
 */
public class DeviceMetric implements Serializable {

    private String deviceId;
    private double temperature;
    /** 事件时间（秒，构造时自动 ×1000 转毫秒） */
    private long eventTime;

    public DeviceMetric() {}

    public DeviceMetric(String deviceId, double temperature, long eventTimeSeconds) {
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.eventTime = eventTimeSeconds * 1000L;
    }

    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }
    public double getTemperature() { return temperature; }
    public void setTemperature(double temperature) { this.temperature = temperature; }
    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }

    @Override
    public String toString() {
        return "DeviceMetric{deviceId='" + deviceId + "', temperature=" + temperature
                + ", ts=" + (eventTime / 1000) + "s}";
    }
}
