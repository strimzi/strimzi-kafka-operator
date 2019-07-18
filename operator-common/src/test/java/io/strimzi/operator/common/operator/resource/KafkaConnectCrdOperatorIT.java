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
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.test.k8s.KubeCluster;
import io.strimzi.test.k8s.NoClusterException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assume;
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
public class KafkaConnectCrdOperatorIT {
    protected static final Logger log = LogManager.getLogger(KafkaConnectCrdOperatorIT.class);

    public static final String RESOURCE_NAME = "my-test-resource";
    protected static Vertx vertx;
    protected static KubernetesClient client;
    protected static CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect> kafkaConnectOperator;
    protected static String namespace;
    protected static String defaultNamespace = "my-test-namespace";

    @BeforeClass
    public static void before() {
        try {
            KubeCluster.bootstrap();
        } catch (NoClusterException e) {
            Assume.assumeTrue(e.getMessage(), false);
        }
        vertx = Vertx.vertx();
        client = new DefaultKubernetesClient();
        kafkaConnectOperator = new CrdOperator(vertx, client, KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class);

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
        client.customResourceDefinitions().create(Crds.kafkaUser());
        log.info("Created CRD");
    }

    @AfterClass
    public static void after() {
        if (client != null) {
            log.info("Deleting CRD");
            client.customResourceDefinitions().delete(Crds.kafkaUser());
        }

        if (vertx != null) {
            vertx.close();
        }
    }

    protected KafkaConnect getResource() {
        return new KafkaConnectBuilder()
                .withApiVersion("kafka.strimzi.io/v1beta1")
                .withNewMetadata()
                .withName(RESOURCE_NAME)
                .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();
    }

    private Future<Void> deleteResource()    {
        // The resource has to be deleted this was and not using reconcile due to https://github.com/fabric8io/kubernetes-client/pull/1325
        // Fix this override when project is using fabric8 version > 4.1.1
        kafkaConnectOperator.operation().inNamespace(namespace).withName(RESOURCE_NAME).delete();

        return kafkaConnectOperator.waitFor(namespace, RESOURCE_NAME, 1_000, 60_000, (ignore1, ignore2) -> {
            KafkaConnect deletion = kafkaConnectOperator.get(namespace, RESOURCE_NAME);
            return deletion == null;
        });
    }

    @Test
    public void testUpdateStatus(TestContext context)    {
        log.info("Getting Kubernetes version");
        Async versionAsync = context.async();
        PlatformFeaturesAvailability.create(vertx, client).setHandler(pfaRes -> {
            if (pfaRes.succeeded())    {
                PlatformFeaturesAvailability pfa = pfaRes.result();
                context.put("pfa", pfa);
                versionAsync.complete();
            } else {
                context.fail(pfaRes.cause());
            }
        });
        versionAsync.awaitSuccess();

        if (((PlatformFeaturesAvailability) context.get("pfa")).getKubernetesVersion().compareTo(KubernetesVersion.V1_11) < 0) {
            log.info("Kubernetes {} is too old", ((PlatformFeaturesAvailability) context.get("pfa")).getKubernetesVersion());
            return;
        }

        log.info("Creating resource");
        Async createAsync = context.async();
        kafkaConnectOperator.reconcile(namespace, RESOURCE_NAME, getResource()).setHandler(res -> {
            if (res.succeeded())    {
                createAsync.complete();
            } else {
                context.fail(res.cause());
            }
        });
        createAsync.awaitSuccess();

        KafkaConnect withStatus = new KafkaConnectBuilder(kafkaConnectOperator.get(namespace, RESOURCE_NAME))
                .withNewStatus()
                .withConditions(new ConditionBuilder()
                        .withType("Ready")
                        .withStatus("True")
                        .build())
                .endStatus()
                .build();

        log.info("Updating resource status");
        Async updateStatusAsync = context.async();
        kafkaConnectOperator.updateStatusAsync(withStatus).setHandler(res -> {
            if (res.succeeded())    {
                kafkaConnectOperator.getAsync(namespace, RESOURCE_NAME).setHandler(res2 -> {
                    if (res2.succeeded())    {
                        KafkaConnect updated = res2.result();

                        context.assertEquals("Ready", updated.getStatus().getConditions().get(0).getType());
                        context.assertEquals("True", updated.getStatus().getConditions().get(0).getStatus());

                        updateStatusAsync.complete();
                    } else {
                        context.fail(res.cause());
                    }
                });
            } else {
                context.fail(res.cause());
            }
        });
        updateStatusAsync.awaitSuccess();

        log.info("Deleting resource");
        Async deleteAsync = context.async();
        deleteResource().setHandler(res -> {
            if (res.succeeded()) {
                deleteAsync.complete();
            } else {
                context.fail(res.cause());
            }
        });
        deleteAsync.awaitSuccess();
    }

    /**
     * Tests what happens when the resource is deleted while updating the status
     *
     * @param context
     */
    @Test
    public void testUpdateStatusWhileResourceDeleted(TestContext context)    {
        log.info("Getting Kubernetes version");
        Async versionAsync = context.async();
        PlatformFeaturesAvailability.create(vertx, client).setHandler(pfaRes -> {
            if (pfaRes.succeeded())    {
                PlatformFeaturesAvailability pfa = pfaRes.result();
                context.put("pfa", pfa);
                versionAsync.complete();
            } else {
                context.fail(pfaRes.cause());
            }
        });
        versionAsync.awaitSuccess();

        if (((PlatformFeaturesAvailability) context.get("pfa")).getKubernetesVersion().compareTo(KubernetesVersion.V1_11) < 0) {
            log.info("Kubernetes {} is too old", ((PlatformFeaturesAvailability) context.get("pfa")).getKubernetesVersion());
            return;
        }

        log.info("Creating resource");
        Async createAsync = context.async();
        kafkaConnectOperator.reconcile(namespace, RESOURCE_NAME, getResource()).setHandler(res -> {
            if (res.succeeded())    {
                createAsync.complete();
            } else {
                context.fail(res.cause());
            }
        });
        createAsync.awaitSuccess();

        KafkaConnect withStatus = new KafkaConnectBuilder(kafkaConnectOperator.get(namespace, RESOURCE_NAME))
                .withNewStatus()
                .withConditions(new ConditionBuilder()
                        .withType("Ready")
                        .withStatus("True")
                        .build())
                .endStatus()
                .build();

        log.info("Deleting resource");
        Async deleteAsync = context.async();
        deleteResource().setHandler(res -> {
            if (res.succeeded()) {
                deleteAsync.complete();
            } else {
                context.fail(res.cause());
            }
        });
        deleteAsync.awaitSuccess();

        log.info("Updating resource status");
        Async updateStatusAsync = context.async();
        kafkaConnectOperator.updateStatusAsync(withStatus).setHandler(res -> {
            context.assertFalse(res.succeeded());
            updateStatusAsync.complete();
        });
        updateStatusAsync.awaitSuccess();
    }

    /**
     * Tests what happens when the resource is modifed while updating the status
     *
     * @param context
     */
    @Test
    public void testUpdateStatusWhileResourceUpdated(TestContext context)    {
        log.info("Getting Kubernetes version");
        Async versionAsync = context.async();
        PlatformFeaturesAvailability.create(vertx, client).setHandler(pfaRes -> {
            if (pfaRes.succeeded())    {
                PlatformFeaturesAvailability pfa = pfaRes.result();
                context.put("pfa", pfa);
                versionAsync.complete();
            } else {
                context.fail(pfaRes.cause());
            }
        });
        versionAsync.awaitSuccess();

        if (((PlatformFeaturesAvailability) context.get("pfa")).getKubernetesVersion().compareTo(KubernetesVersion.V1_11) < 0) {
            log.info("Kubernetes {} is too old", ((PlatformFeaturesAvailability) context.get("pfa")).getKubernetesVersion());
            return;
        }

        log.info("Creating resource");
        Async createAsync = context.async();
        kafkaConnectOperator.reconcile(namespace, RESOURCE_NAME, getResource()).setHandler(res -> {
            if (res.succeeded())    {
                createAsync.complete();
            } else {
                context.fail(res.cause());
            }
        });
        createAsync.awaitSuccess();

        KafkaConnect withStatus = new KafkaConnectBuilder(kafkaConnectOperator.get(namespace, RESOURCE_NAME))
                .withNewStatus()
                .withConditions(new ConditionBuilder()
                        .withType("Ready")
                        .withStatus("True")
                        .build())
                .endStatus()
                .build();

        log.info("Updating resource");
        KafkaConnect updated = new KafkaConnectBuilder(kafkaConnectOperator.get(namespace, RESOURCE_NAME))
                .editSpec()
                .withLogging(new InlineLogging())
                .endSpec()
                .build();

        //Async updateAsync = context.async();
        kafkaConnectOperator.operation().inNamespace(namespace).withName(RESOURCE_NAME).patch(updated);

        log.info("Updating resource status");
        Async updateStatusAsync = context.async();
        kafkaConnectOperator.updateStatusAsync(withStatus).setHandler(res -> {
            context.assertFalse(res.succeeded());
            updateStatusAsync.complete();
        });
        updateStatusAsync.awaitSuccess();

        log.info("Deleting resource");
        Async deleteAsync = context.async();
        deleteResource().setHandler(res -> {
            if (res.succeeded()) {
                deleteAsync.complete();
            } else {
                context.fail(res.cause());
            }
        });
        deleteAsync.awaitSuccess();
    }
}

