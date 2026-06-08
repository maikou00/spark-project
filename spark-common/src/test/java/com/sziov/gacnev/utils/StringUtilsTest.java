package com.sziov.gacnev.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("StringUtils 脱敏与转换测试")
class StringUtilsTest {

    @Test
    void maskPhone() {
        assertThat(StringUtils.maskPhone("13800138000")).isEqualTo("138****8000");
    }

    @Test
    void maskPhoneShort() {
        assertThat(StringUtils.maskPhone("12345")).isEqualTo("12345");
    }

    @Test
    void maskIdCard() {
        String masked = StringUtils.maskIdCard("110101199001011234");
        assertThat(masked).contains("**********");
    }

    @Test
    void maskEmail() {
        assertThat(StringUtils.maskEmail("test@example.com")).isEqualTo("tes****@example.com");
        assertThat(StringUtils.maskEmail("a@b.com")).isEqualTo("****@b.com");
    }

    @Test
    void toUnderlineCase() {
        assertThat(StringUtils.toUnderlineCase("userName")).isEqualTo("user_name");
        assertThat(StringUtils.toUnderlineCase("userNameAge")).isEqualTo("user_name_age");
    }

    @Test
    void truncate() {
        assertThat(StringUtils.truncate("Hello World", 5)).isEqualTo("Hello");
        assertThat(StringUtils.truncate("Hi", 5)).isEqualTo("Hi");
    }
}
