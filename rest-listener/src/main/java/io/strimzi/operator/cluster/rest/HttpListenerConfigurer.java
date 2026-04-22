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
 * <em>different</em> from every other Strimzi listener type:
 *
 * <ul>
 *     <li><b>IS</b> appended to {@code listeners=} — the broker needs to
 *         bind the port.</li>
 *     <li><b>IS NOT</b> appended to {@code listener.security.protocol.map}.
 *         {@code HTTP} / {@code HTTPS} are not Kafka {@code SecurityProtocol}
 *         values. The broker strips them out via
 *         {@code KafkaConfig.httpListeners} before the standard listener
 *         parser runs, so adding a protocol-map entry would cause the
 *         standard parser to fail validation.</li>
 *     <li><b>IS NOT</b> appended to {@code advertised.listeners=}. The
 *         embedded REST proxy does not participate in Kafka's cluster
 *         metadata protocol — external clients discover it through the
 *         Kubernetes {@code Service} / {@code LoadBalancer} (Phase 3),
 *         not through Kafka's binary metadata. Putting {@code HTTP://}
 *         into {@code advertised.listeners} would trip
 *         {@code listenerListToEndPoints} on startup.</li>
 *     <li>Per-listener TLS keystore configuration is handled by the
 *         existing {@code configureTlsOnListener} helper inside
 *         {@code KafkaBrokerConfigurationBuilder} — reused via the wire
 *         name ({@code "HTTPS"}) so the generated
 *         {@code listener.name.https.ssl.keystore.*} keys match what the
 *         broker's {@code KafkaSslContextFactory} looks up via
 *         {@code valuesWithPrefixOverride(listener.name.https.)}.</li>
 * </ul>
 *
 * <p>Called from {@code cluster-operator}'s
 * {@code KafkaBrokerConfigurationBuilder} as a narrow dispatch —
 * everything HTTP-specific lives in this module.</p>
 */
public final class HttpListenerConfigurer {

    private HttpListenerConfigurer() {
    }

    /**
     * Append the HTTP/HTTPS bind entry for {@code listener} to the operator's
     * accumulator list. Does not mutate any other accumulator (not
     * {@code advertisedListeners}, not {@code securityProtocol}); the whole
     * point of this dispatch is that HTTP REST proxy listeners sit outside
     * Kafka's security-protocol machinery.
     *
     * @param listeners  the operator's {@code listeners=} accumulator; mutated in place
     * @param listener   a listener for which
     *                   {@link HttpListenerTypeSupport#isHttpOrHttps} returns {@code true};
     *                   the caller is expected to have filtered.
     * @throws IllegalArgumentException if {@code listener} is not an HTTP / HTTPS listener
     */
    public static void configure(List<String> listeners, GenericKafkaListener listener) {
        final String wireName = HttpListenerTypeSupport.wireName(listener);
        listeners.add(String.format("%s://0.0.0.0:%d", wireName, listener.getPort()));
    }
}
