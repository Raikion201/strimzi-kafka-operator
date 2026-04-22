/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpListenerTypeSupportTest {

    private static GenericKafkaListener listener(KafkaListenerType type) {
        return new GenericKafkaListenerBuilder()
                .withName("l")
                .withPort(8080)
                .withType(type)
                .withTls(type == KafkaListenerType.HTTPS)
                .build();
    }

    @Test
    void httpMatchesOnlyHttp() {
        assertThat(HttpListenerTypeSupport.isHttp(listener(KafkaListenerType.HTTP)), is(true));
        assertThat(HttpListenerTypeSupport.isHttp(listener(KafkaListenerType.HTTPS)), is(false));
        assertThat(HttpListenerTypeSupport.isHttp(listener(KafkaListenerType.INTERNAL)), is(false));
    }

    @Test
    void httpsMatchesOnlyHttps() {
        assertThat(HttpListenerTypeSupport.isHttps(listener(KafkaListenerType.HTTPS)), is(true));
        assertThat(HttpListenerTypeSupport.isHttps(listener(KafkaListenerType.HTTP)), is(false));
        assertThat(HttpListenerTypeSupport.isHttps(listener(KafkaListenerType.LOADBALANCER)), is(false));
    }

    @Test
    void httpOrHttpsCoversBoth() {
        assertThat(HttpListenerTypeSupport.isHttpOrHttps(listener(KafkaListenerType.HTTP)), is(true));
        assertThat(HttpListenerTypeSupport.isHttpOrHttps(listener(KafkaListenerType.HTTPS)), is(true));
        assertThat(HttpListenerTypeSupport.isHttpOrHttps(listener(KafkaListenerType.ROUTE)), is(false));
    }

    @Test
    void predicatesHandleNullSafely() {
        assertThat(HttpListenerTypeSupport.isHttp(null), is(false));
        assertThat(HttpListenerTypeSupport.isHttps(null), is(false));
        assertThat(HttpListenerTypeSupport.isHttpOrHttps(null), is(false));
    }

    @Test
    void wireNameReturnsHttpOrHttpsLiterals() {
        assertThat("Broker regex requires literal HTTP / HTTPS, not the user's listener name",
                HttpListenerTypeSupport.wireName(listener(KafkaListenerType.HTTP)), is("HTTP"));
        assertThat(HttpListenerTypeSupport.wireName(listener(KafkaListenerType.HTTPS)), is("HTTPS"));
    }

    @Test
    void wireNameRejectsNonHttpListener() {
        assertThrows(IllegalArgumentException.class,
                () -> HttpListenerTypeSupport.wireName(listener(KafkaListenerType.INTERNAL)));
    }
}
