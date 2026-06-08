package com.sziov.gacnev.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link StringUtils} 测试用例。
 *
 * @author maikou
 * @since 2026-06-09
 */
@DisplayName("StringUtils 字符串工具测试")
class StringUtilsTest {

    @Test
    @DisplayName("maskPhone_11位手机号_返回138****8000格式")
    void maskPhone_elevenDigit_returnsMaskedFormat() {
        assertThat(StringUtils.maskPhone("13800138000")).isEqualTo("138****8000");
    }

    @Test
    @DisplayName("maskPhone_短号不足7位_原样返回")
    void maskPhone_tooShort_returnsOriginal() {
        assertThat(StringUtils.maskPhone("12345")).isEqualTo("12345");
        assertThat(StringUtils.maskPhone(null)).isNull();
    }

    @Test
    @DisplayName("maskIdCard_18位身份证_中间位脱敏")
    void maskIdCard_eighteenDigit_masksMiddle() {
        String masked = StringUtils.maskIdCard("110101199001011234");
        assertThat(masked).contains("**********");
        assertThat(masked.length()).isEqualTo(18);
    }

    @Test
    @DisplayName("maskEmail_标准邮箱_局部脱敏")
    void maskEmail_standardEmail_masksLocalPart() {
        assertThat(StringUtils.maskEmail("test@example.com")).isEqualTo("tes****@example.com");
    }

    @Test
    @DisplayName("maskEmail_短邮箱_全脱敏")
    void maskEmail_shortEmail_fullMask() {
        assertThat(StringUtils.maskEmail("a@b.com")).isEqualTo("****@b.com");
    }

    @Test
    @DisplayName("toUnderlineCase_驼峰命名_转下划线")
    void toUnderlineCase_camelCase_returnsSnakeCase() {
        assertThat(StringUtils.toUnderlineCase("userName")).isEqualTo("user_name");
        assertThat(StringUtils.toUnderlineCase("userNameAge")).isEqualTo("user_name_age");
        assertThat(StringUtils.toUnderlineCase("user")).isEqualTo("user");
    }

    @Test
    @DisplayName("truncate_字符串超长_截断返回")
    void truncate_exceedsMaxLength_truncates() {
        assertThat(StringUtils.truncate("Hello World", 5)).isEqualTo("Hello");
    }

    @Test
    @DisplayName("truncate_字符串不足最大长度_原样返回")
    void truncate_withinMaxLength_returnsOriginal() {
        assertThat(StringUtils.truncate("Hi", 5)).isEqualTo("Hi");
    }
}
