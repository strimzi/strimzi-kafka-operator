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
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.KubeCluster;
import io.strimzi.test.k8s.exceptions.NoClusterException;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
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

    private static KubeClusterResource cluster;

    @BeforeAll
    public static void before() {
        cluster = KubeClusterResource.getInstance();
        cluster.setTestNamespace(namespace);

        try {
            KubeCluster.bootstrap();
        } catch (NoClusterException e) {
            assumeTrue(false, e.getMessage());
        }
        vertx = Vertx.vertx();
        client = new DefaultKubernetesClient();
        kafkaOperator = new CrdOperator(vertx, client, Kafka.class, KafkaList.class, DoneableKafka.class);

        log.info("Preparing namespace");
        if (cluster.getTestNamespace() != null && System.getenv("SKIP_TEARDOWN") == null) {
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
                .withApiVersion(Kafka.RESOURCE_GROUP + "/" + Kafka.V1BETA1)
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
    public void testUpdateStatus(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        log.info("Getting Kubernetes version");
        CountDownLatch versionAsync = new CountDownLatch(1);
        AtomicReference<PlatformFeaturesAvailability> pfa = new AtomicReference<>();
        PlatformFeaturesAvailability.create(vertx, client).setHandler(pfaRes -> {
            if (pfaRes.succeeded())    {
                pfa.set(pfaRes.result());
                versionAsync.countDown();
            } else {
                context.failNow(pfaRes.cause());
            }
        });
        if (!versionAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        if (pfa.get().getKubernetesVersion().compareTo(KubernetesVersion.V1_11) < 0) {
            log.info("Kubernetes {} is too old", pfa.get().getKubernetesVersion());
            return;
        }

        log.info("Creating resource");
        CountDownLatch createAsync = new CountDownLatch(1);
        kafkaOperator.reconcile(namespace, RESOURCE_NAME, getResource()).setHandler(res -> {
            if (res.succeeded())    {
                createAsync.countDown();
            } else {
                context.failNow(res.cause());
            }
        });
        if (!createAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        Kafka withStatus = new KafkaBuilder(kafkaOperator.get(namespace, RESOURCE_NAME))
                .withNewStatus()
                .withConditions(new ConditionBuilder()
                        .withType("Ready")
                        .withStatus("True")
                        .build())
                .endStatus()
                .build();

        log.info("Updating resource status");
        CountDownLatch updateStatusAsync = new CountDownLatch(1);
        kafkaOperator.updateStatusAsync(withStatus).setHandler(res -> {
            if (res.succeeded())    {
                kafkaOperator.getAsync(namespace, RESOURCE_NAME).setHandler(res2 -> {
                    if (res2.succeeded())    {
                        Kafka updated = res2.result();

                        context.verify(() -> assertThat(updated.getStatus().getConditions().get(0).getType(), is("Ready")));
                        context.verify(() -> assertThat(updated.getStatus().getConditions().get(0).getStatus(), is("True")));

                        updateStatusAsync.countDown();
                    } else {
                        context.failNow(res.cause());
                    }
                });
            } else {
                context.failNow(res.cause());
            }
        });
        if (!updateStatusAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        log.info("Deleting resource");
        CountDownLatch deleteAsync = new CountDownLatch(1);
        kafkaOperator.reconcile(namespace, RESOURCE_NAME, null).setHandler(res -> {
            if (res.succeeded()) {
                deleteAsync.countDown();
            } else {
                context.failNow(res.cause());
            }
        });
        if (!deleteAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }
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
        CountDownLatch versionAsync = new CountDownLatch(1);
        AtomicReference<PlatformFeaturesAvailability> pfa = new AtomicReference<>();
        PlatformFeaturesAvailability.create(vertx, client).setHandler(pfaRes -> {
            if (pfaRes.succeeded())    {
                pfa.set(pfaRes.result());
                versionAsync.countDown();
            } else {
                context.failNow(pfaRes.cause());
            }
        });
        if (!versionAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        if (pfa.get().getKubernetesVersion().compareTo(KubernetesVersion.V1_11) < 0) {
            log.info("Kubernetes {} is too old", pfa.get().getKubernetesVersion());
            return;
        }

        log.info("Creating resource");
        CountDownLatch createAsync = new CountDownLatch(1);
        kafkaOperator.reconcile(namespace, RESOURCE_NAME, getResource()).setHandler(res -> {
            if (res.succeeded())    {
                createAsync.countDown();
            } else {
                context.failNow(res.cause());
            }
        });
        if (!createAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        Kafka withStatus = new KafkaBuilder(kafkaOperator.get(namespace, RESOURCE_NAME))
                .withNewStatus()
                .withConditions(new ConditionBuilder()
                        .withType("Ready")
                        .withStatus("True")
                        .build())
                .endStatus()
                .build();

        log.info("Deleting resource");
        CountDownLatch deleteAsync = new CountDownLatch(1);
        kafkaOperator.reconcile(namespace, RESOURCE_NAME, null).setHandler(res -> {
            if (res.succeeded()) {
                deleteAsync.countDown();
            } else {
                context.failNow(res.cause());
            }
        });
        if (!deleteAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        log.info("Updating deleted resource status");
        CountDownLatch updateStatusAsync = new CountDownLatch(1);
        kafkaOperator.updateStatusAsync(withStatus).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(false)));
            updateStatusAsync.countDown();
        });
        if (!updateStatusAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

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
        CountDownLatch versionAsync = new CountDownLatch(1);
        AtomicReference<PlatformFeaturesAvailability> pfa = new AtomicReference<>();
        PlatformFeaturesAvailability.create(vertx, client).setHandler(pfaRes -> {
            if (pfaRes.succeeded())    {
                pfa.set(pfaRes.result());
                versionAsync.countDown();
            } else {
                context.failNow(pfaRes.cause());
            }
        });
        if (!versionAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        if (pfa.get().getKubernetesVersion().compareTo(KubernetesVersion.V1_11) < 0) {
            log.info("Kubernetes {} is too old", pfa.get().getKubernetesVersion());
            return;
        }

        log.info("Creating resource");
        CountDownLatch createAsync = new CountDownLatch(1);
        kafkaOperator.reconcile(namespace, RESOURCE_NAME, getResource()).setHandler(res -> {
            if (res.succeeded())    {
                createAsync.countDown();
            } else {
                context.failNow(res.cause());
            }
        });
        if (!createAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

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
        CountDownLatch updateStatusAsync = new CountDownLatch(1);
        kafkaOperator.updateStatusAsync(withStatus).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(false)));
            updateStatusAsync.countDown();
        });
        if (!updateStatusAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        log.info("Deleting resource");
        CountDownLatch deleteAsync = new CountDownLatch(1);
        kafkaOperator.reconcile(namespace, RESOURCE_NAME, null).setHandler(res -> {
            if (res.succeeded()) {
                deleteAsync.countDown();
            } else {
                context.failNow(res.cause());
            }
        });
        if (!deleteAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }
        context.completeNow();
    }
}

