package com.flinkcourse.common.model;

import java.io.Serializable;

/**
 * 转账事件（第12章 反欺诈实战）
 */
public class TransferEvent implements Serializable {

    private long accountId;
    private double amount;
    /** 事件时间（毫秒） */
    private long ts;

    public TransferEvent() {}

    public TransferEvent(long accountId, double amount, long tsMillis) {
        this.accountId = accountId;
        this.amount = amount;
        this.ts = tsMillis;
    }

    public long getAccountId() { return accountId; }
    public void setAccountId(long accountId) { this.accountId = accountId; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    public long getTs() { return ts; }
    public void setTs(long ts) { this.ts = ts; }

    @Override
    public String toString() {
        return "TransferEvent{accountId=" + accountId + ", amount=" + amount + ", ts=" + ts + "}";
    }
}
