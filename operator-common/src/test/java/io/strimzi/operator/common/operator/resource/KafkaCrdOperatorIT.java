/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.test.k8s.KubeCluster;
import io.strimzi.test.k8s.NoClusterException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.strimzi.test.BaseITST.cmdKubeClient;
import static io.strimzi.test.BaseITST.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
public class KafkaCrdOperatorIT {
    protected static final Logger log = LogManager.getLogger(KafkaCrdOperatorIT.class);

    public static final String RESOURCE_NAME = "my-test-resource";
    protected static Vertx vertx;
    protected static KubernetesClient client;
    protected static CrdOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka> kafkaOperator;
    protected static String namespace = "kafka-crd-it-namespace";

    @BeforeAll
    public static void before() {
        try {
            KubeCluster.bootstrap();
        } catch (NoClusterException e) {
            assumeTrue(false, e.getMessage());
        }
        vertx = Vertx.vertx();
        client = new DefaultKubernetesClient();
        kafkaOperator = new CrdOperator(vertx, client, Kafka.class, KafkaList.class, DoneableKafka.class);

        log.info("Preparing namespace");
        if (kubeClient().getNamespace(namespace) != null && System.getenv("SKIP_TEARDOWN") == null) {
            log.warn("Namespace {} is already created, going to delete it", namespace);
            kubeClient().deleteNamespace(namespace);
            cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
        }

        log.info("Creating namespace: {}", namespace);
        kubeClient().createNamespace(namespace);
        cmdKubeClient().waitForResourceCreation("Namespace", namespace);

        log.info("Creating CRD");
        client.customResourceDefinitions().create(Crds.kafka());
        log.info("Created CRD");
    }

    @AfterAll
    public static void after() {
        if (client != null) {
            log.info("Deleting CRD");
            client.customResourceDefinitions().delete(Crds.kafka());
        }
        if (kubeClient().getNamespace(namespace) != null && System.getenv("SKIP_TEARDOWN") == null) {
            log.warn("Deleting namespace {} after tests run", namespace);
            kubeClient().deleteNamespace(namespace);
            cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
        }

        if (vertx != null) {
            vertx.close();
        }
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

    private Future<Void> deleteResource()    {
        // The resource has to be deleted this was and not using reconcile due to https://github.com/fabric8io/kubernetes-client/pull/1325
        // Fix this override when project is using fabric8 version > 4.1.1
        kafkaOperator.operation().inNamespace(namespace).withName(RESOURCE_NAME).delete();

        return kafkaOperator.waitFor(namespace, RESOURCE_NAME, 1_000, 60_000, (ignore1, ignore2) -> {
            Kafka deletion = kafkaOperator.get(namespace, RESOURCE_NAME);
            return deletion == null;
        });
    }

    @Test
    public void testUpdateStatus(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        log.info("Getting Kubernetes version");
        CompletableFuture<Boolean> versionAsync = new CompletableFuture<>();
        AtomicReference<PlatformFeaturesAvailability> pfa = new AtomicReference<>();
        PlatformFeaturesAvailability.create(vertx, client).setHandler(pfaRes -> {
            if (pfaRes.succeeded())    {
                pfa.set(pfaRes.result());
                versionAsync.complete(true);
            } else {
                context.failNow(pfaRes.cause());
            }
        });
        versionAsync.get(60, TimeUnit.SECONDS);

        if (pfa.get().getKubernetesVersion().compareTo(KubernetesVersion.V1_11) < 0) {
            log.info("Kubernetes {} is too old", pfa.get().getKubernetesVersion());
            return;
        }

        log.info("Creating resource");
        CompletableFuture<Boolean> createAsync = new CompletableFuture<>();
        kafkaOperator.reconcile(namespace, RESOURCE_NAME, getResource()).setHandler(res -> {
            if (res.succeeded())    {
                createAsync.complete(true);
            } else {
                context.failNow(res.cause());
            }
        });
        createAsync.get(60, TimeUnit.SECONDS);

        Kafka withStatus = new KafkaBuilder(kafkaOperator.get(namespace, RESOURCE_NAME))
                .withNewStatus()
                .withConditions(new ConditionBuilder()
                        .withType("Ready")
                        .withStatus("True")
                        .build())
                .endStatus()
                .build();

        log.info("Updating resource status");
        CompletableFuture<Boolean> updateStatusAsync = new CompletableFuture<>();
        kafkaOperator.updateStatusAsync(withStatus).setHandler(res -> {
            if (res.succeeded())    {
                kafkaOperator.getAsync(namespace, RESOURCE_NAME).setHandler(res2 -> {
                    if (res2.succeeded())    {
                        Kafka updated = res2.result();

                        context.verify(() -> assertThat(updated.getStatus().getConditions().get(0).getType(), is("Ready")));
                        context.verify(() -> assertThat(updated.getStatus().getConditions().get(0).getStatus(), is("True")));

                        updateStatusAsync.complete(true);
                    } else {
                        context.failNow(res.cause());
                    }
                });
            } else {
                context.failNow(res.cause());
            }
        });
        updateStatusAsync.get(60, TimeUnit.SECONDS);

        log.info("Deleting resource");
        CompletableFuture<Boolean> deleteAsync = new CompletableFuture<>();
        deleteResource().setHandler(res -> {
            if (res.succeeded()) {
                deleteAsync.complete(true);
            } else {
                context.failNow(res.cause());
            }
        });
        deleteAsync.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }

    /**
     * Tests what happens when the resource is deleted while updating the status
     *
     * @param context
     */
    @Test
    public void testUpdateStatusWhileResourceDeleted(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        log.info("Getting Kubernetes version");
        CompletableFuture<Boolean> versionAsync = new CompletableFuture<>();
        AtomicReference<PlatformFeaturesAvailability> pfa = new AtomicReference<>();
        PlatformFeaturesAvailability.create(vertx, client).setHandler(pfaRes -> {
            if (pfaRes.succeeded())    {
                pfa.set(pfaRes.result());
                versionAsync.complete(true);
            } else {
                context.failNow(pfaRes.cause());
            }
        });
        versionAsync.get(60, TimeUnit.SECONDS);

        if (pfa.get().getKubernetesVersion().compareTo(KubernetesVersion.V1_11) < 0) {
            log.info("Kubernetes {} is too old", pfa.get().getKubernetesVersion());
            return;
        }

        log.info("Creating resource");
        CompletableFuture<Boolean> createAsync = new CompletableFuture<>();
        kafkaOperator.reconcile(namespace, RESOURCE_NAME, getResource()).setHandler(res -> {
            if (res.succeeded())    {
                createAsync.complete(true);
            } else {
                context.failNow(res.cause());
            }
        });
        createAsync.get(60, TimeUnit.SECONDS);

        Kafka withStatus = new KafkaBuilder(kafkaOperator.get(namespace, RESOURCE_NAME))
                .withNewStatus()
                .withConditions(new ConditionBuilder()
                        .withType("Ready")
                        .withStatus("True")
                        .build())
                .endStatus()
                .build();

        log.info("Deleting resource");
        CompletableFuture<Boolean> deleteAsync = new CompletableFuture<>();
        deleteResource().setHandler(res -> {
            if (res.succeeded()) {
                deleteAsync.complete(true);
            } else {
                context.failNow(res.cause());
            }
        });
        deleteAsync.get(60, TimeUnit.SECONDS);

        log.info("Updating deleted resource status");
        CompletableFuture<Boolean> updateStatusAsync = new CompletableFuture<>();
        kafkaOperator.updateStatusAsync(withStatus).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(false)));
            updateStatusAsync.complete(false);
        });
        updateStatusAsync.get(60, TimeUnit.SECONDS);

        context.completeNow();
    }

    /**
     * Tests what happens when the resource is modifed while updating the status
     *
     * @param context
     */
    @Test
    public void testUpdateStatusWhileResourceUpdated(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        log.info("Getting Kubernetes version");
        CompletableFuture<Boolean> versionAsync = new CompletableFuture<>();
        AtomicReference<PlatformFeaturesAvailability> pfa = new AtomicReference<>();
        PlatformFeaturesAvailability.create(vertx, client).setHandler(pfaRes -> {
            if (pfaRes.succeeded())    {
                pfa.set(pfaRes.result());
                versionAsync.complete(true);
            } else {
                context.failNow(pfaRes.cause());
            }
        });
        versionAsync.get(60, TimeUnit.SECONDS);

        if (pfa.get().getKubernetesVersion().compareTo(KubernetesVersion.V1_11) < 0) {
            log.info("Kubernetes {} is too old", pfa.get().getKubernetesVersion());
            return;
        }

        log.info("Creating resource");
        CompletableFuture<Boolean> createAsync = new CompletableFuture<>();
        kafkaOperator.reconcile(namespace, RESOURCE_NAME, getResource()).setHandler(res -> {
            if (res.succeeded())    {
                createAsync.complete(true);
            } else {
                context.failNow(res.cause());
            }
        });
        createAsync.get(60, TimeUnit.SECONDS);

        Kafka withStatus = new KafkaBuilder(kafkaOperator.get(namespace, RESOURCE_NAME))
                .withNewStatus()
                .withConditions(new ConditionBuilder()
                        .withType("Ready")
                        .withStatus("True")
                        .build())
                .endStatus()
                .build();

        log.info("Updating resource");
        Kafka updated = new KafkaBuilder(kafkaOperator.get(namespace, RESOURCE_NAME))
                .editSpec()
                    .editKafka()
                        .addToConfig("xxx", "yyy")
                    .endKafka()
                .endSpec()
                .build();

        //Async updateAsync = context.async();
        kafkaOperator.operation().inNamespace(namespace).withName(RESOURCE_NAME).patch(updated);
        /*kafkaOperator.reconcile(namespace, RESOURCE_NAME, updated).setHandler(res -> {
            if (res.succeeded())    {
                updateAsync.complete();
            } else {
                context.fail(res.cause());
            }
        });
        updateAsync.awaitSuccess();*/

        log.info("Updating resource status");
        CompletableFuture<Boolean> updateStatusAsync = new CompletableFuture<>();
        kafkaOperator.updateStatusAsync(withStatus).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(false)));
            updateStatusAsync.complete(true);
        });
        updateStatusAsync.get(60, TimeUnit.SECONDS);

        log.info("Deleting resource");
        CompletableFuture<Boolean> deleteAsync = new CompletableFuture<>();
        deleteResource().setHandler(res -> {
            if (res.succeeded()) {
                deleteAsync.complete(true);
            } else {
                context.failNow(res.cause());
            }
        });
        deleteAsync.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }
}

