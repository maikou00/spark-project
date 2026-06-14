package com.sziov.gacnev.utils;

/**
 * 字符串工具类 - 仅包含自定义增强方法。
 *
 * <p>通用字符串操作请直接使用 {@link org.apache.commons.lang3.StringUtils}。</p>
 *
 * @author maikou
 * @since 2026-05-17
 */
public final class StringUtils {

    private StringUtils() {}

    /** 手机号脱敏：138****8000 */
    public static String maskPhone(String phone) {
        if (org.apache.commons.lang3.StringUtils.isBlank(phone) || phone.length() < 11) {
            return phone;
        }
        return phone.substring(0, 3) + "****" + phone.substring(7);
    }

    /** 身份证号脱敏：1101**********1234 */
    public static String maskIdCard(String idCard) {
        if (org.apache.commons.lang3.StringUtils.isBlank(idCard) || idCard.length() < 15) {
            return idCard;
        }
        return idCard.substring(0, 4) + "**********" + idCard.substring(14);
    }

    /** 邮箱脱敏：tes****@example.com */
    public static String maskEmail(String email) {
        if (org.apache.commons.lang3.StringUtils.isBlank(email) || !email.contains("@")) {
            return email;
        }
        int atIndex = email.indexOf("@");
        if (atIndex <= 3) {
            return "****" + email.substring(atIndex);
        }
        return email.substring(0, 3) + "****" + email.substring(atIndex);
    }

    /** 驼峰转下划线：userName → user_name */
    public static String toUnderlineCase(String str) {
        if (org.apache.commons.lang3.StringUtils.isBlank(str)) {
            return str;
        }
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    result.append('_');
                }
                result.append(Character.toLowerCase(c));
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    /** 安全截取字符串 */
    public static String truncate(String str, int length) {
        if (str == null || length <= 0) {
            return str;
        }
        return str.length() > length ? str.substring(0, length) : str;
    }
}
