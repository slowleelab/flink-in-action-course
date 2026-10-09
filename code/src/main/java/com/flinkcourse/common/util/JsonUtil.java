package com.flinkcourse.common.util;

import com.flinkcourse.common.model.Order;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 极简 JSON 工具（只处理课程使用的扁平 JSON，无第三方依赖）
 * 生产请使用 Jackson/Fastjson2 —— 这里刻意手写，避免教学代码被序列化库细节淹没
 */
public final class JsonUtil {

    private JsonUtil() {}

    /** 取字符串字段："key":"value" */
    public static String getStr(String json, String key) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*\"([^\"]*)\"").matcher(json);
        return m.find() ? m.group(1) : null;
    }

    /** 取数值字段（返回 double；缺失返回 0） */
    public static double getDouble(String json, String key) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*(-?\\d+(?:\\.\\d+)?)").matcher(json);
        return m.find() ? Double.parseDouble(m.group(1)) : 0.0;
    }

    public static long getLong(String json, String key) {
        return (long) getDouble(json, key);
    }

    public static int getInt(String json, String key) {
        return (int) getDouble(json, key);
    }

    /** JSON 字符串 → Order；解析失败返回 null（由上层 filter 兜底） */
    public static Order toOrder(String json) {
        try {
            String phone = getStr(json, "phone");
            return new Order(
                    getLong(json, "userId"),
                    getDouble(json, "amount"),
                    getStr(json, "city"),
                    getInt(json, "status"),
                    phone == null ? "" : phone,
                    getLong(json, "ts"));
        } catch (Exception e) {
            return null;
        }
    }
}
