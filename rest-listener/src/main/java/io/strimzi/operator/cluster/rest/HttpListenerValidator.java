/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * CR-level validation rules specific to HTTP / HTTPS REST proxy listeners.
 * Delegated to by {@code cluster-operator}'s {@code ListenersValidator} so
 * the HTTP-specific rules live next to the rest of the HTTP code rather
 * than scattered across the validator's switch-chain.
 *
 * <p>Rules enforced here:</p>
 * <ul>
 *     <li><b>HTTPS must have {@code tls: true}.</b></li>
 *     <li><b>HTTP must have {@code tls: false}.</b></li>
 *     <li><b>Kafka-protocol authentication is not applicable.</b> REST
 *         authentication is HTTP Basic via
 *         {@code http.rest.basic.credentials}.</li>
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
     *                 listeners produce an empty result set.
     * @return the set of human-readable error messages; empty when valid.
     */
    public static Set<String> validate(final GenericKafkaListener listener) {
        if (!HttpListenerTypeSupport.isHttpOrHttps(listener)) {
            return Collections.emptySet();
        }

        Set<String> errors = new HashSet<>();
        final String prefix = "listener '" + listener.getName() + "'";

        if (HttpListenerTypeSupport.isHttps(listener) && !listener.isTls()) {
            errors.add(prefix
                    + " has type 'https' but 'tls: false';"
                    + " HTTPS listeners must set tls: true");
        }
        if (HttpListenerTypeSupport.isHttp(listener) && listener.isTls()) {
            errors.add(prefix
                    + " has type 'http' but 'tls: true';"
                    + " plain HTTP listeners must set tls: false");
        }
        if (listener.getAuth() != null) {
            errors.add(prefix
                    + " has listener-level auth configured, which does not"
                    + " apply to HTTP REST proxy listeners."
                    + " Use http.rest.basic.credentials in"
                    + " spec.kafka.config for HTTP Basic auth instead");
        }

        return errors;
    }

    /**
     * Checks that the Kafka version in use supports the embedded REST proxy.
     *
     * @param listeners              full list of CR listeners
     * @param versionSupportsRestProxy whether the version includes the proxy
     * @param kafkaVersion           the version string, for diagnostics
     * @return error set; empty when compatible
     */
    public static Set<String> checkKafkaVersionSupport(
            final List<GenericKafkaListener> listeners,
            final boolean versionSupportsRestProxy,
            final String kafkaVersion) {
        boolean hasRestListener = listeners != null
                && listeners.stream()
                        .anyMatch(HttpListenerTypeSupport::isHttpOrHttps);
        if (!hasRestListener || versionSupportsRestProxy) {
            return Collections.emptySet();
        }
        return Set.of(
                "Kafka version '" + kafkaVersion + "' does not include"
                + " the embedded HTTP REST proxy, so listeners with type"
                + " 'http' / 'https' cannot be served."
                + " Either choose a Kafka version whose entry in"
                + " kafka-versions.yaml has 'supports-rest-proxy: true',"
                + " or remove the HTTP/HTTPS listeners from the Kafka CR.");
    }
}
