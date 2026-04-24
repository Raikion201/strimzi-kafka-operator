/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.listener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Enumerates the supported Kafka listener types in Strimzi CRs. */
public enum KafkaListenerType {
    /** Internal cluster listener (no external exposure). */
    INTERNAL,
    /** OpenShift Route listener. */
    ROUTE,
    /** Kubernetes LoadBalancer listener. */
    LOADBALANCER,
    /** Kubernetes NodePort listener. */
    NODEPORT,
    /** Kubernetes Ingress listener. */
    INGRESS,
    /** Kubernetes ClusterIP listener. */
    CLUSTER_IP,
    /** Embedded HTTP REST proxy listener. */
    HTTP,
    /** Embedded HTTPS REST proxy listener. */
    HTTPS;

    /**
     * Deserialise from the JSON string value used in CRs.
     *
     * @param value the JSON string
     * @return the matching enum constant, or {@code null} if unknown
     */
    @JsonCreator
    public static KafkaListenerType forValue(final String value) {
        switch (value) {
            case "internal":
                return INTERNAL;
            case "route":
                return ROUTE;
            case "loadbalancer":
                return LOADBALANCER;
            case "nodeport":
                return NODEPORT;
            case "ingress":
                return INGRESS;
            case "cluster-ip":
                return CLUSTER_IP;
            case "http":
                return HTTP;
            case "https":
                return HTTPS;
            default:
                return null;
        }
    }

    /**
     * Serialise to the JSON string value used in CRs.
     *
     * @return the JSON string for this constant
     */
    @JsonValue
    public String toValue() {
        switch (this) {
            case INTERNAL:
                return "internal";
            case ROUTE:
                return "route";
            case LOADBALANCER:
                return "loadbalancer";
            case NODEPORT:
                return "nodeport";
            case INGRESS:
                return "ingress";
            case CLUSTER_IP:
                return "cluster-ip";
            case HTTP:
                return "http";
            case HTTPS:
                return "https";
            default:
                return null;
        }
    }
}
