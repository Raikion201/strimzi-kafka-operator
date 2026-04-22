/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpListenerConfigurerTest {

    @Test
    void httpListenerIsAppendedAsHttpLiteralOn0000() {
        GenericKafkaListener http = new GenericKafkaListenerBuilder()
                .withName("rest")
                .withPort(8080)
                .withType(KafkaListenerType.HTTP)
                .withTls(false)
                .build();

        List<String> listeners = new ArrayList<>();
        HttpListenerConfigurer.configure(listeners, http);

        assertThat("HTTP listener must bind 0.0.0.0 on the configured port",
                listeners, contains("HTTP://0.0.0.0:8080"));
    }

    @Test
    void httpsListenerIsAppendedAsHttpsLiteralOn0000() {
        GenericKafkaListener https = new GenericKafkaListenerBuilder()
                .withName("rest-tls")
                .withPort(8443)
                .withType(KafkaListenerType.HTTPS)
                .withTls(true)
                .build();

        List<String> listeners = new ArrayList<>();
        HttpListenerConfigurer.configure(listeners, https);

        assertThat(listeners, contains("HTTPS://0.0.0.0:8443"));
    }

    @Test
    void configureDoesNotMutateOtherCollectionsOrUseAnyOtherArguments() {
        // The whole point of HTTP listeners is that they do NOT touch
        // securityProtocol or advertised.listeners. If this test starts
        // failing because the method signature widened to take those,
        // that's probably a regression to watch.
        GenericKafkaListener http = new GenericKafkaListenerBuilder()
                .withName("rest")
                .withPort(8080)
                .withType(KafkaListenerType.HTTP)
                .withTls(false)
                .build();

        List<String> listeners = new ArrayList<>();
        HttpListenerConfigurer.configure(listeners, http);

        assertThat("only one entry appended", listeners, hasSize(1));
        assertThat(listeners, hasItem("HTTP://0.0.0.0:8080"));
    }

    @Test
    void nonHttpListenerIsRejected() {
        GenericKafkaListener internal = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();

        assertThrows(IllegalArgumentException.class,
                () -> HttpListenerConfigurer.configure(new ArrayList<>(), internal),
                "Passing a non-HTTP listener must fail fast; caller was expected to filter.");
    }
}
