/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IBuilder;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.KubeCluster;
import io.strimzi.test.k8s.exceptions.NoClusterException;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
public class KafkaConnectS2ICrdOperatorIT {
    protected static final Logger log = LogManager.getLogger(KafkaConnectS2ICrdOperatorIT.class);

    public static final String RESOURCE_NAME = "my-test-resource";
    protected static Vertx vertx;
    protected static KubernetesClient client;
    protected static CrdOperator<KubernetesClient, KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I> kafkaConnectS2IOperator;
    protected static String namespace = "connects2i-crd-it-namespace";

    private static  KubeClusterResource cluster;

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
        kafkaConnectS2IOperator = new CrdOperator(vertx, client, KafkaConnectS2I.class, KafkaConnectS2IList.class, DoneableKafkaConnectS2I.class);

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
        client.customResourceDefinitions().create(Crds.kafkaConnectS2I());
        log.info("Created CRD");
    }

    @AfterAll
    public static void after() {
        if (client != null) {
            log.info("Deleting CRD");
            client.customResourceDefinitions().delete(Crds.kafkaConnectS2I());
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

    protected KafkaConnectS2I getResource() {
        return new KafkaConnectS2IBuilder()
                .withApiVersion(KafkaConnectS2I.RESOURCE_GROUP + "/" + KafkaConnectS2I.V1BETA1)
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

    @Test
    public void testUpdateStatus(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        log.info("Getting Kubernetes version");
        PlatformFeaturesAvailability.create(vertx, client)
            .setHandler(context.succeeding(pfa -> context.verify(() -> {
                assertThat("Kubernetes version : " + pfa.getKubernetesVersion() + " is too old",
                        pfa.getKubernetesVersion().compareTo(KubernetesVersion.V1_11), is(not(lessThan(0))));
            })))

            .compose(pfa -> {
                log.info("Creating resource");
                return kafkaConnectS2IOperator.reconcile(namespace, RESOURCE_NAME, getResource());
            })
            .setHandler(context.succeeding())

            .compose(rrCreated -> {
                KafkaConnectS2I newStatus = new KafkaConnectS2IBuilder(kafkaConnectS2IOperator.get(namespace, RESOURCE_NAME))
                        .withNewStatus()
                        .withConditions(new ConditionBuilder()
                                .withType("Ready")
                                .withStatus("True")
                                .build())
                        .endStatus()
                        .build();

                log.info("Updating resource status");
                return kafkaConnectS2IOperator.updateStatusAsync(newStatus);
            })
            .setHandler(context.succeeding())

            .compose(rrModified -> kafkaConnectS2IOperator.getAsync(namespace, RESOURCE_NAME))
            .setHandler(context.succeeding(modifiedKafkaConnectS2I -> context.verify(() -> {
                assertThat(modifiedKafkaConnectS2I.getStatus().getConditions().get(0).getType(), is("Ready"));
                assertThat(modifiedKafkaConnectS2I.getStatus().getConditions().get(0).getStatus(), is("True"));
            })))

            .compose(rrModified -> {
                log.info("Deleting resource");
                return kafkaConnectS2IOperator.reconcile(namespace, RESOURCE_NAME, null);
            })
            .setHandler(context.succeeding(rrDeleted ->  async.flag()));
    }

    /**
     * Tests what happens when the resource is deleted while updating the status
     *
     * @param context
     */
    @Test
    public void testUpdateStatusWhileResourceDeletedThrowsNullPointerException(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        log.info("Getting Kubernetes version");
        PlatformFeaturesAvailability.create(vertx, client)
            .setHandler(context.succeeding(pfa -> context.verify(() -> {
                assertThat("Kubernetes version : " + pfa.getKubernetesVersion() + " is too old",
                        pfa.getKubernetesVersion().compareTo(KubernetesVersion.V1_11), is(not(lessThan(0))));
            })))
            .compose(pfa -> {
                log.info("Creating resource");
                return kafkaConnectS2IOperator.reconcile(namespace, RESOURCE_NAME, getResource());
            })
            .setHandler(context.succeeding())

            .compose(rr -> {
                log.info("Deleting resource");
                return kafkaConnectS2IOperator.reconcile(namespace, RESOURCE_NAME, null);
            })
            .setHandler(context.succeeding())

            .compose(v -> {
                KafkaConnectS2I newStatus = new KafkaConnectS2IBuilder(kafkaConnectS2IOperator.get(namespace, RESOURCE_NAME))
                        .withNewStatus()
                        .withConditions(new ConditionBuilder()
                                .withType("Ready")
                                .withStatus("True")
                                .build())
                        .endStatus()
                        .build();

                log.info("Updating resource status");
                return kafkaConnectS2IOperator.updateStatusAsync(newStatus);
            })
            .setHandler(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(NullPointerException.class));
                async.flag();
            })));
    }

    /**
     * Tests what happens when the resource is modified while updating the status
     *
     * @param context
     */
    @Test
    public void testUpdateStatusThrowsKubernetesExceptionIfResourceUpdatedPrior(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        Promise updateFailed = Promise.promise();

        log.info("Getting Kubernetes version");
        PlatformFeaturesAvailability.create(vertx, client)
            .setHandler(context.succeeding(pfa -> context.verify(() -> {
                assertThat("Kubernetes version : " + pfa.getKubernetesVersion() + " is too old",
                        pfa.getKubernetesVersion().compareTo(KubernetesVersion.V1_11), is(not(lessThan(0))));
            })))
            .compose(pfa -> {
                log.info("Creating resource");
                return kafkaConnectS2IOperator.reconcile(namespace, RESOURCE_NAME, getResource());
            })
            .setHandler(context.succeeding())
            .compose(rr -> {
                KafkaConnectS2I currentKafkaConnectS2I = kafkaConnectS2IOperator.get(namespace, RESOURCE_NAME);

                KafkaConnectS2I updated = new KafkaConnectS2IBuilder(currentKafkaConnectS2I)
                        .editSpec()
                            .withNewLivenessProbe()
                                .withInitialDelaySeconds(14)
                            .endLivenessProbe()
                        .endSpec()
                        .build();


                KafkaConnectS2I newStatus = new KafkaConnectS2IBuilder(currentKafkaConnectS2I)
                        .withNewStatus()
                        .withConditions(new ConditionBuilder()
                                .withType("Ready")
                                .withStatus("True")
                                .build())
                        .endStatus()
                        .build();

                log.info("Updating resource (mocking an update due to some other reason)");
                kafkaConnectS2IOperator.operation().inNamespace(namespace).withName(RESOURCE_NAME).patch(updated);

                log.info("Updating resource status after underlying resource has changed");
                return kafkaConnectS2IOperator.updateStatusAsync(newStatus);
            })
            .setHandler(context.failing(e -> context.verify(() -> {
                System.out.println("please");
                assertThat("Exception was not KubernetesClientException, it was : " + e.toString(),
                        e, instanceOf(KubernetesClientException.class));
                updateFailed.complete();
            })));

        updateFailed.future().compose(v -> {
            log.info("Deleting resource");
            return kafkaConnectS2IOperator.reconcile(namespace, RESOURCE_NAME, null);
        })
        .setHandler(context.succeeding(v -> async.flag()));
    }
}

