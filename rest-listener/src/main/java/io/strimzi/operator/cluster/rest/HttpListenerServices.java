/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;

/**
 * Policy helpers for the Kubernetes resources that expose an HTTP or HTTPS
 * REST proxy listener. Called by {@code cluster-operator} as a narrow
 * dispatch — {@code cluster-operator} never imports concrete
 * Service-generation code from here.
 *
 * <ul>
 *     <li>Default Service type is {@code LoadBalancer} so external clients
 *         have somewhere to land.</li>
 *     <li>Per-broker Services are skipped — the REST proxy is stateless
 *         with respect to partition ownership, so one bootstrap Service
 *         that round-robins across broker pods is sufficient.</li>
 * </ul>
 */
public final class HttpListenerServices {

    /** Default Kubernetes {@code Service.spec.type} for HTTP/HTTPS. */
    private static final String DEFAULT_SERVICE_TYPE = "LoadBalancer";

    private HttpListenerServices() {
    }

    /**
     * Whether this module is responsible for the given listener's Service.
     *
     * @param listener the listener to classify
     * @return {@code true} when this module handles the listener's Service
     */
    public static boolean handles(final GenericKafkaListener listener) {
        return HttpListenerTypeSupport.isHttpOrHttps(listener);
    }

    /**
     * Kubernetes {@code Service.spec.type} for an HTTP/HTTPS bootstrap
     * Service.
     *
     * @param listener the listener (must satisfy {@link #handles})
     * @return {@code "LoadBalancer"}
     * @throws IllegalArgumentException if not an HTTP / HTTPS listener
     */
    public static String serviceType(final GenericKafkaListener listener) {
        if (!handles(listener)) {
            throw new IllegalArgumentException(
                    "serviceType called on a non-HTTP listener: " + listener);
        }
        return DEFAULT_SERVICE_TYPE;
    }

    /**
     * Whether the per-pod Services loop should skip this listener.
     * Per-broker Services are unnecessary for HTTP REST requests.
     *
     * @param listener the listener to classify
     * @return {@code true} when the per-pod loop should skip this listener
     */
    public static boolean skipPerPodService(
            final GenericKafkaListener listener) {
        return HttpListenerTypeSupport.isHttpOrHttps(listener);
    }
}
