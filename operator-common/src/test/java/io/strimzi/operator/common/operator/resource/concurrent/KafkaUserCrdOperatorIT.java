/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.concurrent;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ForkJoinPool;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
public class KafkaUserCrdOperatorIT extends AbstractCustomResourceOperatorIT<KubernetesClient, KafkaUser, KafkaUserList> {
    protected static final Logger LOGGER = LogManager.getLogger(KafkaUserCrdOperatorIT.class);

    @Override
    protected CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> operator() {
        return new CrdOperator<>(ForkJoinPool.commonPool(), client, KafkaUser.class, KafkaUserList.class, KafkaUser.RESOURCE_KIND);
    }

    @Override
    protected String getCrd() {
        return TestUtils.CRD_KAFKA_USER;
    }

    @Override
    protected String getCrdName() {
        return KafkaUser.CRD_NAME;
    }

    @Override
    protected String getNamespace() {
        return "kafka-user-crd-it-namespace";
    }

    @Override
    protected KafkaUser getResource(String resourceName) {
        return new KafkaUserBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(getNamespace())
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();
    }

    @Override
    protected KafkaUser getResourceWithModifications(KafkaUser resourceInCluster) {
        return new KafkaUserBuilder(resourceInCluster)
                .editSpec()
                    .withNewKafkaUserTlsClientAuthentication().endKafkaUserTlsClientAuthentication()
                .endSpec()
                .build();
    }

    @Override
    protected KafkaUser getResourceWithNewReadyStatus(KafkaUser resourceInCluster) {
        return new KafkaUserBuilder(resourceInCluster)
                .withNewStatus()
                    .withConditions(READY_CONDITION)
                .endStatus()
                .build();
    }

    @Override
    protected void assertReady(KafkaUser resource) {
        assertThat(resource.getStatus()
                .getConditions()
                .get(0), is(READY_CONDITION));
    }
}
