/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;

/**
 * Tiny predicates that let {@code cluster-operator} dispatch HTTP/HTTPS
 * listener work to this module without ever importing the concrete renderers.
 * The rest of this module owns all HTTP-specific behaviour; {@code cluster-operator}
 * only needs to ask "is this listener mine to handle?".
 */
public final class HttpListenerTypeSupport {

    private HttpListenerTypeSupport() {
    }

    /**
     * Classifies a listener as a plain-HTTP REST proxy listener.
     *
     * @param listener the listener to classify
     * @return {@code true} if the listener is a plain HTTP REST proxy listener.
     */
    public static boolean isHttp(GenericKafkaListener listener) {
        return listener != null && KafkaListenerType.HTTP == listener.getType();
    }

    /**
     * Classifies a listener as an HTTPS REST proxy listener.
     *
     * @param listener the listener to classify
     * @return {@code true} if the listener is an HTTPS REST proxy listener.
     */
    public static boolean isHttps(GenericKafkaListener listener) {
        return listener != null && KafkaListenerType.HTTPS == listener.getType();
    }

    /**
     * Classifies a listener as any REST proxy listener (HTTP or HTTPS).
     *
     * @param listener the listener to classify
     * @return {@code true} if the listener is an HTTP or HTTPS REST proxy listener.
     */
    public static boolean isHttpOrHttps(GenericKafkaListener listener) {
        return isHttp(listener) || isHttps(listener);
    }

    /**
     * Canonical on-the-wire listener name for an HTTP/HTTPS listener. The Kafka
     * broker's {@code KafkaConfig.HttpListenerRegex} matches exactly {@code HTTP}
     * and {@code HTTPS} (case-insensitive), so the operator must emit those
     * literals rather than the user-chosen listener name.
     *
     * @param listener HTTP or HTTPS listener
     * @return {@code "HTTPS"} if {@link #isHttps(GenericKafkaListener)}, otherwise {@code "HTTP"}
     * @throws IllegalArgumentException if the listener is neither HTTP nor HTTPS
     */
    public static String wireName(GenericKafkaListener listener) {
        if (isHttps(listener)) {
            return "HTTPS";
        }
        if (isHttp(listener)) {
            return "HTTP";
        }
        throw new IllegalArgumentException("wireName called on a non-HTTP listener: " + listener);
    }
}
