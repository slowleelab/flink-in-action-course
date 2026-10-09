package com.flinkcourse.common.model;

import java.io.Serializable;

/**
 * 用户行为事件（第6章 状态管理实战）
 * 埋点格式：userId + 行为类型（view/cart/order）+ 商品 + 事件时间（秒）
 */
public class UserAction implements Serializable {

    private long userId;
    private String action;
    private String sku;
    private long ts;

    public UserAction() {}

    public UserAction(long userId, String action, String sku, long ts) {
        this.userId = userId;
        this.action = action;
        this.sku = sku;
        this.ts = ts;
    }

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }
    public String getSku() { return sku; }
    public void setSku(String sku) { this.sku = sku; }
    public long getTs() { return ts; }
    public void setTs(long ts) { this.ts = ts; }

    @Override
    public String toString() {
        return "UserAction{userId=" + userId + ", action='" + action
                + "', sku='" + sku + "', ts=" + ts + "}";
    }
}
