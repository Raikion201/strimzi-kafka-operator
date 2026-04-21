/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;

import java.util.List;

/**
 * Renders the {@code HTTP://} / {@code HTTPS://} fragments into the broker
 * {@code server.properties}. Unlike every other Strimzi listener type, an
 * HTTP REST proxy listener appears only in {@code listeners=} — it must not
 * appear in {@code listener.security.protocol.map} because the broker's
 * {@code KafkaConfig.httpListeners} filter strips HTTP/HTTPS entries before
 * the standard listener parser runs.
 *
 * <p>This class is called by
 * {@code cluster-operator}'s {@code KafkaBrokerConfigurationBuilder} as a
 * narrow dispatch — the operator passes in the partially-built
 * {@code listeners} list and we append our own entries to it without touching
 * {@code securityProtocol}.</p>
 *
 * <p>Real logic arrives in Phase 2 of the integration plan. The Phase-1
 * stub keeps the operator side compilable while the CRD and enum changes
 * land first.</p>
 */
public final class HttpListenerConfigurer {

    private HttpListenerConfigurer() {
    }

    /**
     * Append the {@code HTTP://} / {@code HTTPS://} bind entry for this listener.
     * Intentionally does NOT mutate the caller's {@code securityProtocol} list —
     * that's the whole point of separating these listeners out.
     *
     * @param listeners          the operator's {@code listeners=} accumulator; mutated in place
     * @param advertisedListeners the operator's {@code advertised.listeners=} accumulator; mutated in place
     * @param listener           a listener that {@link HttpListenerTypeSupport#isHttpOrHttps} returns {@code true} for
     * @param advertisedHostname the DNS/IP the listener should advertise — resolved by the operator
     */
    public static void configure(List<String> listeners,
                                 List<String> advertisedListeners,
                                 GenericKafkaListener listener,
                                 String advertisedHostname) {
        final String wireName = HttpListenerTypeSupport.wireName(listener);
        final int port = listener.getPort();

        listeners.add(String.format("%s://0.0.0.0:%d", wireName, port));
        advertisedListeners.add(String.format("%s://%s:%d", wireName, advertisedHostname, port));
        // DELIBERATELY no entry added to listener.security.protocol.map.
    }
}
