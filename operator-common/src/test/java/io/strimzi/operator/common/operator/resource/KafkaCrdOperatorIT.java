/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@RunWith(VertxUnitRunner.class)
public class KafkaCrdOperatorIT {
    protected static final Logger log = LogManager.getLogger(KafkaCrdOperatorIT.class);

    public static final String RESOURCE_NAME = "my-test-resource";
    protected static Vertx vertx;
    protected static KubernetesClient client;
    protected static String namespace;
    protected static String defaultNamespace = "my-test-namespace";

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
        client = new DefaultKubernetesClient();

        log.info("Preparing namespace");
        namespace = client.getNamespace();
        if (namespace == null) {
            Namespace ns = client.namespaces().withName(defaultNamespace).get();

            if (ns == null) {
                client.namespaces().create(new NamespaceBuilder()
                        .withNewMetadata()
                        .withName(defaultNamespace)
                        .endMetadata()
                        .build());
            }

            namespace = defaultNamespace;
        }

        log.info("Creating CRD");
        client.customResourceDefinitions().create(Crds.kafka());
        log.info("Created CRD");
    }

    @AfterClass
    public static void after() {
        log.info("Deleting CRD");
        client.customResourceDefinitions().delete(Crds.kafka());

        vertx.close();
    }

    protected Kafka getResource() {
        return new KafkaBuilder()
                .withApiVersion("kafka.strimzi.io/v1beta1")
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(1)
                        .withNewListeners()
                            .withNewPlain()
                            .endPlain()
                        .endListeners()
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(1)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();
    }

    @Test
    public void testUpdateStatus(TestContext context)    {
        log.info("Creating operator");
        CrdOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka> kafkaOperator = new CrdOperator(vertx, client, Kafka.class, KafkaList.class, DoneableKafka.class);

        log.info("Creating resource");
        Async createAsync = context.async();
        kafkaOperator.reconcile(namespace, RESOURCE_NAME, getResource()).setHandler(res -> {
            if (res.succeeded())    {
                createAsync.complete();
            } else {
                context.fail(res.cause());
                createAsync.complete();
            }
        });
        createAsync.awaitSuccess();

        Kafka withStatus = new KafkaBuilder(kafkaOperator.get(namespace, RESOURCE_NAME))
                .withNewStatus()
                    .withConditions(new ConditionBuilder()
                            .withType("Ready")
                            .withStatus("True")
                            .build())
                .endStatus()
                .build();

        log.info("Updating resource status");
        Async updateStatusAsync = context.async();
        kafkaOperator.updateStatusAsync(withStatus).setHandler(res -> {
            if (res.succeeded())    {
                kafkaOperator.getAsync(namespace, RESOURCE_NAME).setHandler(res2 -> {
                    if (res2.succeeded())    {
                        Kafka updated = res2.result();

                        context.assertEquals("Ready", updated.getStatus().getConditions().get(0).getType());
                        context.assertEquals("True", updated.getStatus().getConditions().get(0).getStatus());

                        updateStatusAsync.complete();
                    } else {
                        context.fail(res.cause());
                        updateStatusAsync.complete();
                    }
                });
            } else {
                context.fail(res.cause());
            }
        });
        updateStatusAsync.awaitSuccess();
    }
}

