/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;

import java.util.List;

/**
 * Renders the {@code HTTP://} / {@code HTTPS://} fragments of the broker
 * {@code server.properties}. An HTTP REST proxy listener is deliberately
 * different from every other Strimzi listener type:
 *
 * <ul>
 *     <li><b>IS</b> appended to {@code listeners=}.</li>
 *     <li><b>IS NOT</b> appended to
 *         {@code listener.security.protocol.map}: {@code HTTP} and
 *         {@code HTTPS} are not Kafka {@code SecurityProtocol} values.</li>
 *     <li><b>IS NOT</b> appended to {@code advertised.listeners=}:
 *         the REST proxy does not participate in Kafka's cluster metadata
 *         protocol.</li>
 * </ul>
 *
 * <p>Called from {@code cluster-operator}'s
 * {@code KafkaBrokerConfigurationBuilder} as a narrow dispatch.</p>
 */
public final class HttpListenerConfigurer {

    private HttpListenerConfigurer() {
    }

    /**
     * Append the HTTP/HTTPS bind entry for {@code listener} to the
     * operator's accumulator list. Does not mutate any other accumulator.
     *
     * @param listeners the operator's {@code listeners=} accumulator;
     *                  mutated in place
     * @param listener  an HTTP or HTTPS listener; the caller is expected
     *                  to have filtered via
     *                  {@link HttpListenerTypeSupport#isHttpOrHttps}
     * @throws IllegalArgumentException if not an HTTP / HTTPS listener
     */
    public static void configure(
            final List<String> listeners,
            final GenericKafkaListener listener) {
        final String wireName = HttpListenerTypeSupport.wireName(listener);
        listeners.add(
                String.format("%s://0.0.0.0:%d", wireName, listener.getPort()));
    }
}
