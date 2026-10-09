package com.flinkcourse.common.model;

import java.io.Serializable;

/**
 * 订单实体（贯穿第3/5/8/11章）
 * POJO 规范：public 类 + public 无参构造 + public getter/setter（Flink 序列化要求）
 */
public class Order implements Serializable {

    /** 订单状态：1 待支付 */
    public static final int STATUS_PENDING = 1;
    /** 订单状态：2 已支付（GMV 只统计已支付） */
    public static final int STATUS_PAID = 2;
    /** 订单状态：3 已取消 */
    public static final int STATUS_CANCELLED = 3;

    private long userId;
    private double amount;
    private String city;
    private int status;
    private String phone;
    /** 事件时间（毫秒时间戳）—— Watermark 依据 */
    private long eventTime;

    public Order() {}

    public Order(long userId, double amount, String city, int status, long eventTime) {
        this(userId, amount, city, status, "", eventTime);
    }

    public Order(long userId, double amount, String city, int status, String phone, long eventTime) {
        this.userId = userId;
        this.amount = amount;
        this.city = city;
        this.status = status;
        this.phone = phone;
        this.eventTime = eventTime;
    }

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }
    public int getStatus() { return status; }
    public void setStatus(int status) { this.status = status; }
    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }
    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }

    @Override
    public String toString() {
        return "Order{userId=" + userId + ", amount=" + amount + ", city='" + city
                + "', status=" + status + ", phone='" + phone + "', eventTime=" + eventTime + "}";
    }
}
