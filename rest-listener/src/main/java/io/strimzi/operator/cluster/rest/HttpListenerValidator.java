/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;

import java.util.Collections;
import java.util.List;

/**
 * CR-level validation specific to HTTP / HTTPS listeners. Delegated to by
 * {@code cluster-operator}'s {@code ListenersValidator} so the validation
 * rules that apply to the REST proxy (HTTPS requires {@code tls: true},
 * HTTP forbids it, authentication types other than HTTP Basic don't apply
 * here, etc.) live next to the code they protect.
 *
 * <p>Phase 4 of the integration plan fills in real rules. The Phase-1 stub
 * returns an empty error list so the dispatch call site can be wired up
 * without blocking the CRD/enum work.</p>
 */
public final class HttpListenerValidator {

    private HttpListenerValidator() {
    }

    /**
     * Validate a single HTTP / HTTPS listener. Returns an empty list when
     * everything checks out; otherwise a list of human-readable error
     * messages in the same shape the rest of {@code ListenersValidator}
     * produces.
     *
     * @param listener an HTTP or HTTPS listener (callers should have filtered)
     * @return list of validation error messages; empty when the listener is valid
     */
    public static List<String> validate(GenericKafkaListener listener) {
        if (!HttpListenerTypeSupport.isHttpOrHttps(listener)) {
            return Collections.emptyList();
        }
        // Phase 4 fills in: HTTPS must have tls=true, HTTP must have tls=false,
        // incompatible auth types are rejected, etc.
        return Collections.emptyList();
    }
}
