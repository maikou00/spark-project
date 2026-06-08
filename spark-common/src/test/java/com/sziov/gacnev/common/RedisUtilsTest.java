package com.sziov.gacnev.common;

import com.sziov.gacnev.AbstractSparkTest;
import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RedisUtilsTest extends AbstractSparkTest {

    @Test
    void shouldBuildRedisUriFromConfig() {
        RedisURI uri = RedisUtils.buildRedisUri();
        assertThat(uri.getHost()).isEqualTo("localhost");
        assertThat(uri.getPort()).isEqualTo(6379);
        assertThat(uri.getDatabase()).isEqualTo(0);
    }
}
