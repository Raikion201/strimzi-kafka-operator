/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * CR-level validation rules specific to HTTP / HTTPS REST proxy listeners.
 * Delegated to by {@code cluster-operator}'s {@code ListenersValidator} so
 * the HTTP-specific rules live next to the rest of the HTTP code rather
 * than scattered across the validator's switch-chain.
 *
 * <p>Rules enforced here:</p>
 * <ul>
 *     <li><b>HTTPS must have {@code tls: true}.</b> An HTTPS listener with
 *         {@code tls: false} would skip Jetty's TLS connector wiring and
 *         silently accept plaintext — dangerous and inconsistent with the
 *         broker-side {@code KafkaSslContextFactory} expectations.</li>
 *     <li><b>HTTP must have {@code tls: false}.</b> The broker's
 *         {@code KafkaConfig.HttpListenerRegex} matches {@code HTTP://}
 *         and {@code HTTPS://} literally by scheme; {@code tls: true} on
 *         an HTTP listener would write an {@code HTTP://} wire literal
 *         plus a TLS config, an inconsistent combination.</li>
 *     <li><b>Kafka-protocol authentication is not applicable.</b> REST
 *         authentication is HTTP Basic via
 *         {@code http.rest.basic.credentials}; SASL/mTLS/OAuth listener
 *         auth doesn't apply — reject at the CR to avoid silent drops.</li>
 * </ul>
 */
public final class HttpListenerValidator {

    private HttpListenerValidator() {
    }

    /**
     * Validate a single HTTP / HTTPS listener.
     *
     * @param listener an HTTP or HTTPS listener (callers should filter via
     *                 {@link HttpListenerTypeSupport#isHttpOrHttps}); non-HTTP
     *                 listeners produce an empty result set so the delegate
     *                 call site is a safe no-op for other types.
     * @return the set of human-readable error messages; empty when the listener
     *         is valid. Return type matches {@code ListenersValidator}'s own
     *         error collector.
     */
    public static Set<String> validate(GenericKafkaListener listener) {
        if (!HttpListenerTypeSupport.isHttpOrHttps(listener)) {
            return Collections.emptySet();
        }

        Set<String> errors = new HashSet<>();
        final String prefix = "listener '" + listener.getName() + "'";

        if (HttpListenerTypeSupport.isHttps(listener) && !listener.isTls()) {
            errors.add(prefix + " has type 'https' but 'tls: false'; HTTPS listeners must set tls: true");
        }
        if (HttpListenerTypeSupport.isHttp(listener) && listener.isTls()) {
            errors.add(prefix + " has type 'http' but 'tls: true'; plain HTTP listeners must set tls: false");
        }
        if (listener.getAuth() != null) {
            errors.add(prefix
                    + " has listener-level auth configured, which does not apply to HTTP REST proxy listeners. "
                    + "Use http.rest.basic.credentials in spec.kafka.config for HTTP Basic auth instead");
        }

        return errors;
    }
}
