/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.stringContainsInOrder;

class HttpListenerValidatorTest {

    @Test
    void validHttpListenerYieldsNoErrors() {
        GenericKafkaListener http = new GenericKafkaListenerBuilder()
                .withName("rest")
                .withPort(8080)
                .withType(KafkaListenerType.HTTP)
                .withTls(false)
                .build();

        assertThat(HttpListenerValidator.validate(http), is(empty()));
    }

    @Test
    void validHttpsListenerYieldsNoErrors() {
        GenericKafkaListener https = new GenericKafkaListenerBuilder()
                .withName("rest-tls")
                .withPort(8443)
                .withType(KafkaListenerType.HTTPS)
                .withTls(true)
                .build();

        assertThat(HttpListenerValidator.validate(https), is(empty()));
    }

    @Test
    void httpsWithoutTlsIsRejected() {
        GenericKafkaListener bad = new GenericKafkaListenerBuilder()
                .withName("rest-tls")
                .withPort(8443)
                .withType(KafkaListenerType.HTTPS)
                .withTls(false)
                .build();

        Set<String> errors = HttpListenerValidator.validate(bad);
        assertThat(errors, hasSize(1));
        assertThat(errors, hasItem(stringContainsInOrder("rest-tls", "type 'https'", "tls: false")));
    }

    @Test
    void httpWithTlsIsRejected() {
        GenericKafkaListener bad = new GenericKafkaListenerBuilder()
                .withName("rest")
                .withPort(8080)
                .withType(KafkaListenerType.HTTP)
                .withTls(true)
                .build();

        Set<String> errors = HttpListenerValidator.validate(bad);
        assertThat(errors, hasSize(1));
        assertThat(errors, hasItem(stringContainsInOrder("rest", "type 'http'", "tls: true")));
    }

    @Test
    void listenerLevelAuthIsRejected() {
        GenericKafkaListener bad = new GenericKafkaListenerBuilder()
                .withName("rest")
                .withPort(8080)
                .withType(KafkaListenerType.HTTP)
                .withTls(false)
                .withAuth(new KafkaListenerAuthenticationTls())
                .build();

        Set<String> errors = HttpListenerValidator.validate(bad);
        assertThat(errors, hasSize(1));
        assertThat(errors, hasItem(stringContainsInOrder("rest", "listener-level auth", "http.rest.basic.credentials")));
    }

    @Test
    void nonHttpListenerIsIgnoredAsNoOp() {
        // Ensures the validator stays safe to call unconditionally from the
        // existing ListenersValidator per-listener loop.
        GenericKafkaListener internal = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        assertThat(HttpListenerValidator.validate(internal), is(empty()));
    }
}
