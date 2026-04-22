/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;

/**
 * Policy helpers for the Kubernetes resources that expose an HTTP or HTTPS
 * REST proxy listener. Called by {@code cluster-operator} (specifically
 * {@code ListenersUtils} / {@code KafkaCluster}) as a narrow dispatch —
 * {@code cluster-operator} never imports concrete Service-generation code
 * from here, it just asks the questions this class answers.
 *
 * <p>The actual Service construction still runs through the shared
 * {@code ServiceUtils.createService(...)} path; this class only
 * overrides the per-listener-type decisions that differ for HTTP / HTTPS:</p>
 *
 * <ul>
 *     <li>Default Service type is {@code LoadBalancer} so a {@code curl}
 *         from outside the cluster has somewhere to land — that is the
 *         explicit stakeholder requirement. Users who want internal-only
 *         HTTP access can still override via the standard
 *         {@code spec.kafka.template} mechanism; the default is tuned
 *         for the demo.</li>
 *     <li>Per-broker Services are <em>skipped</em>. The REST proxy is
 *         stateless with respect to partition ownership — every broker
 *         accepts every request and forwards to the partition leader via
 *         the normal produce path. One bootstrap Service that round-robins
 *         across broker pods is sufficient, whereas the Kafka binary
 *         protocol needs per-broker addressability for client partition
 *         selection.</li>
 * </ul>
 */
public final class HttpListenerServices {

    /** Default Kubernetes {@code Service.spec.type} for an HTTP / HTTPS REST listener. */
    private static final String DEFAULT_SERVICE_TYPE = "LoadBalancer";

    private HttpListenerServices() {
    }

    /**
     * Should {@code cluster-operator}'s Service generator dispatch here
     * for the given listener? Mirrors the predicate used at the dispatch
     * site so the two are always in lock-step.
     *
     * @param listener the listener to classify
     * @return {@code true} when this module is responsible for the listener's Service shape
     */
    public static boolean handles(GenericKafkaListener listener) {
        return HttpListenerTypeSupport.isHttpOrHttps(listener);
    }

    /**
     * Kubernetes {@code Service.spec.type} string to use for an HTTP / HTTPS
     * REST listener's bootstrap Service.
     *
     * @param listener the listener (must satisfy {@link #handles})
     * @return {@code "LoadBalancer"}
     */
    public static String serviceType(GenericKafkaListener listener) {
        if (!handles(listener)) {
            throw new IllegalArgumentException("serviceType called on a non-HTTP listener: " + listener);
        }
        return DEFAULT_SERVICE_TYPE;
    }

    /**
     * Should the Per-Pod Services loop in {@code KafkaCluster.generatePerPodServices}
     * skip this listener? Per-broker Services only make sense when clients
     * need to address a specific broker (Kafka binary protocol's
     * partition-leader selection). HTTP REST requests work against any
     * broker, so per-broker Services are unnecessary overhead.
     *
     * @param listener the listener to classify
     * @return {@code true} when the per-pod loop should skip this listener
     */
    public static boolean skipPerPodService(GenericKafkaListener listener) {
        return HttpListenerTypeSupport.isHttpOrHttps(listener);
    }
}
