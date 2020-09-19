/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
public class KafkaUserCrdOperatorIT extends AbstractCustomResourceOperatorIT<KubernetesClient, KafkaUser, KafkaUserList, DoneableKafkaUser> {
    protected static final Logger log = LogManager.getLogger(KafkaUserCrdOperatorIT.class);

    @Override
    protected CrdOperator operator() {
        return new CrdOperator(vertx, client, KafkaUser.class, KafkaUserList.class, DoneableKafkaUser.class, Crds.kafkaUser());
    }

    @Override
    protected CustomResourceDefinition getCrd() {
        return Crds.kafkaUser();
    }

    @Override
    protected String getNamespace() {
        return "kafka-user-crd-it-namespace";
    }

    @Override
    protected KafkaUser getResource(String resourceName) {
        return new KafkaUserBuilder()
                .withApiVersion(KafkaUser.RESOURCE_GROUP + "/" + KafkaUser.V1BETA1)
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
    protected void assertReady(VertxTestContext context, KafkaUser resource) {
        context.verify(() -> assertThat(resource.getStatus()
                .getConditions()
                .get(0), is(READY_CONDITION)));
    }
}

