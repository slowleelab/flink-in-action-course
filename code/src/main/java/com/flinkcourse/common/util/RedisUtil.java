package com.flinkcourse.common.util;

/**
 * Redis 连接配置（全课程统一入口）
 *
 * 默认 localhost:6379（无密码）。复用本机已有 Redis（如 lumio-redis）时通常无需任何设置；
 * 地址不同则用环境变量覆盖：
 *
 *   export REDIS_HOST=localhost REDIS_PORT=16379
 */
public final class RedisUtil {

    /** Redis 主机 */
    public static final String HOST =
            System.getenv().getOrDefault("REDIS_HOST", "localhost");

    /** Redis 端口 */
    public static final int PORT =
            Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));

    private RedisUtil() {
    }
}
