/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;

/**
 * Factory for the Kubernetes {@code Service} / {@code Ingress} / OpenShift
 * {@code Route} resources that expose an HTTP or HTTPS REST proxy listener.
 *
 * <p>Called from {@code cluster-operator}'s {@code KafkaCluster} when it
 * iterates user-configured listeners and encounters an HTTP/HTTPS entry.
 * The operator hands over the listener plus the common labels/owner-reference
 * context; this class returns the shaped-up resource object ready to be
 * reconciled.</p>
 *
 * <p>Real implementation arrives in Phase 3 — the end state is that
 * {@code kubectl apply} of a {@code Kafka} CR with {@code type: http} and
 * {@code configuration.bootstrap.loadBalancerIP} produces a
 * {@code Service type: LoadBalancer} targeting the broker pods on the
 * listener's port, allocated by the cluster's cloud provider.</p>
 */
public final class HttpListenerServices {

    private HttpListenerServices() {
    }

    /**
     * Is this listener one that this module knows how to expose?
     * Mirrors the predicate {@code cluster-operator} uses to decide whether
     * to dispatch Service generation here.
     *
     * @param listener the listener to classify
     * @return {@code true} when {@code cluster-operator} should delegate to this module
     */
    public static boolean handles(GenericKafkaListener listener) {
        return HttpListenerTypeSupport.isHttpOrHttps(listener);
    }
}
