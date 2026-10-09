-- =====================================================================
-- Flink 实战课程 - MySQL 初始化脚本（容器首次启动时自动执行）
-- 用途：
--   1. orders 表   —— 第9章 CDC 演示源表 / 第10章 JDBC Sink 目标表
--   2. dws_gmv     —— 第8/11章 Flink SQL 写入的实时结果表
-- =====================================================================
USE flink_course;

-- ---------- 订单表：CDC 源表 ----------
CREATE TABLE IF NOT EXISTS orders (
    order_id     BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id      BIGINT       NOT NULL COMMENT '用户ID',
    amount       DECIMAL(10,2) NOT NULL COMMENT '订单金额',
    city         VARCHAR(32)  NOT NULL COMMENT '下单城市',
    status       TINYINT      NOT NULL DEFAULT 1 COMMENT '1待支付 2已支付 3已取消',
    create_time  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_user (user_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT ='订单表';

-- 预置一批订单数据，供 CDC 捕获
INSERT INTO orders (user_id, amount, city, status) VALUES
(1001, 299.00, '深圳', 2),
(1002, 59.90,  '杭州', 1),
(1001, 1299.00,'深圳', 2),
(1003, 89.00,  '北京', 3),
(1002, 399.00, '杭州', 2),
(1004, 25.50,  '成都', 2),
(1005, 899.00, '上海', 1);

-- ---------- DWS 层结果表：实时 GMV ----------
CREATE TABLE IF NOT EXISTS dws_gmv (
    stat_time   VARCHAR(64)  NOT NULL COMMENT '统计窗口标识',
    city        VARCHAR(32)  NOT NULL DEFAULT '' COMMENT '城市',
    order_cnt   BIGINT       NOT NULL DEFAULT 0 COMMENT '订单数',
    gmv_amount  DECIMAL(16,2) NOT NULL DEFAULT 0 COMMENT 'GMV',
    update_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (stat_time, city)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT ='实时GMV结果表';

-- ---------- 风控告警结果表 ----------
CREATE TABLE IF NOT EXISTS risk_alert (
    alert_id    BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    account_id  BIGINT       NOT NULL COMMENT '账户ID',
    rule        VARCHAR(64)  NOT NULL COMMENT '命中规则',
    detail      VARCHAR(255) DEFAULT '' COMMENT '告警详情',
    create_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_account (account_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT ='风控告警表';

-- ---------- 城市维表：第10章 Lookup Join 演示 ----------
CREATE TABLE IF NOT EXISTS dim_city (
    city_id     BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    city_name   VARCHAR(32)  NOT NULL,
    province    VARCHAR(32)  NOT NULL
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT ='城市维表';

INSERT INTO dim_city (city_name, province) VALUES
('深圳', '广东省'), ('杭州', '浙江省'), ('北京', '北京市'), ('上海', '上海市'), ('成都', '四川省'), ('武汉', '湖北省');

-- 授权（CDC 需要专用账号，演示直接用 root，生产严禁如此）
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'%';
FLUSH PRIVILEGES;
