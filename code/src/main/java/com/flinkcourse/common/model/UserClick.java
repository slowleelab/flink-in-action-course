package com.flinkcourse.common.model;

import java.io.Serializable;

/**
 * 用户点击事件（第5章 滑动窗口 UV / 会话窗口实战）
 */
public class UserClick implements Serializable {

    private long userId;
    private String action;
    /** 事件时间（秒，构造时自动 ×1000 转毫秒） */
    private long eventTime;

    public UserClick() {}

    public UserClick(long userId, String action, long eventTimeSeconds) {
        this.userId = userId;
        this.action = action;
        this.eventTime = eventTimeSeconds * 1000L;
    }

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }
    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }

    @Override
    public String toString() {
        return "UserClick{userId=" + userId + ", action='" + action + "', eventTime=" + eventTime + "}";
    }
}
