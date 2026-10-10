package com.flinkcourse.common.util;

/**
 * MySQL 连接配置（全课程统一入口）
 *
 * 默认 root/flink123@localhost:3306/flink_course（课程 docker 独立部署时的默认值）。
 * 复用本机已有 MySQL 时，运行前设置环境变量即可，无需改代码：
 *
 *   export MYSQL_USER=xxx MYSQL_PASSWORD=yyy
 *   # 库名/端口不同还可以：export MYSQL_URL_BASE=localhost:13306 MYSQL_DB=flink_course
 *
 * 与 KafkaUtil 一样，这是"连接信息外置为配置"的生产实践。
 */
public final class MysqlUtil {

    /** 主机名（CDC 的 hostname 参数，不含端口） */
    public static final String HOST_BASE =
            System.getenv().getOrDefault("MYSQL_HOST", "localhost");

    /** 端口（CDC 的 port 参数，字符串形式） */
    public static final String PORT =
            System.getenv().getOrDefault("MYSQL_PORT", "3306");

    /** JDBC 连接 URL 前缀：jdbc:mysql://host:port/ */
    public static final String URL_PREFIX = "jdbc:mysql://" + HOST_BASE + ":" + PORT + "/";

    /** 用户名 */
    public static final String USER =
            System.getenv().getOrDefault("MYSQL_USER", "root");

    /** 密码 */
    public static final String PASSWORD =
            System.getenv().getOrDefault("MYSQL_PASSWORD", "flink123");

    private MysqlUtil() {
    }
}
