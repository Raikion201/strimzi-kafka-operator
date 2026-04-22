/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.rest;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpListenerServicesTest {

    private static GenericKafkaListener listener(KafkaListenerType type) {
        return new GenericKafkaListenerBuilder()
                .withName("l")
                .withPort(8080)
                .withType(type)
                .withTls(type == KafkaListenerType.HTTPS)
                .build();
    }

    @Test
    void handlesAgreesWithTypeSupportPredicate() {
        assertThat(HttpListenerServices.handles(listener(KafkaListenerType.HTTP)), is(true));
        assertThat(HttpListenerServices.handles(listener(KafkaListenerType.HTTPS)), is(true));
        assertThat(HttpListenerServices.handles(listener(KafkaListenerType.INTERNAL)), is(false));
        assertThat(HttpListenerServices.handles(listener(KafkaListenerType.LOADBALANCER)), is(false));
    }

    @Test
    void httpListenerDefaultsToLoadBalancerService() {
        assertThat("HTTP default Service type must be LoadBalancer so the stakeholder-demo curl from outside "
                        + "the cluster lands somewhere reachable",
                HttpListenerServices.serviceType(listener(KafkaListenerType.HTTP)), is("LoadBalancer"));
    }

    @Test
    void httpsListenerDefaultsToLoadBalancerService() {
        assertThat(HttpListenerServices.serviceType(listener(KafkaListenerType.HTTPS)), is("LoadBalancer"));
    }

    @Test
    void serviceTypeRejectsNonHttpListener() {
        assertThrows(IllegalArgumentException.class,
                () -> HttpListenerServices.serviceType(listener(KafkaListenerType.INTERNAL)));
    }

    @Test
    void skipPerPodServiceIsTrueForHttpListeners() {
        // Per-broker Services make sense only when clients need to address a
        // specific broker. REST clients hit any broker, so no need.
        assertThat(HttpListenerServices.skipPerPodService(listener(KafkaListenerType.HTTP)), is(true));
        assertThat(HttpListenerServices.skipPerPodService(listener(KafkaListenerType.HTTPS)), is(true));
    }

    @Test
    void skipPerPodServiceIsFalseForOtherListenerTypes() {
        // Don't regress the per-broker Service loop for other types.
        assertThat(HttpListenerServices.skipPerPodService(listener(KafkaListenerType.LOADBALANCER)), is(false));
        assertThat(HttpListenerServices.skipPerPodService(listener(KafkaListenerType.NODEPORT)), is(false));
        assertThat(HttpListenerServices.skipPerPodService(listener(KafkaListenerType.INTERNAL)), is(false));
    }
}
