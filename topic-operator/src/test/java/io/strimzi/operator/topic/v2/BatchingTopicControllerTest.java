/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.testcontainers.TestcontainersKafkaCluster;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.KubeCluster;
import io.strimzi.test.k8s.exceptions.NoClusterException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * This test is not intended to provide lots of coverage of the {@link BatchingTopicController}
 * (see {@link TopicControllerIT} for that), rather it aims to cover some parts that a difficult
 * to test via {@link TopicControllerIT}
 */
@ExtendWith(KafkaClusterExtension.class)
class BatchingTopicControllerTest {

    private static final Logger LOGGER = LogManager.getLogger(BatchingTopicControllerTest.class);

    static final String NAMESPACE = "ns";
    static final String NAME = "foo";

    private BatchingTopicController controller;
    private KubernetesClient client;

    private Admin[] admin = new Admin[] {null};

    @BeforeEach
    public void createKubClient() {
        this.client = new KubernetesClientBuilder().build();
    }

    private static <T> KafkaFuture<T> interruptedFuture() throws ExecutionException, InterruptedException {
        var future = mock(KafkaFuture.class);
        doThrow(new InterruptedException()).when(future).get();
        return future;
    }

    private String namespace(String ns) {
        //namespaces.push(ns);
        Resource<Namespace> resource = client.namespaces().withName(ns);
        if (resource.get() == null) {
            LOGGER.debug("Creating namespace {}", ns);
            client.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(ns).endMetadata().build()).create();
            waitUntilCondition(client.namespaces().withName(ns), Objects::nonNull);
        }
        return ns;
    }

    private KafkaTopic createResource() {
        var kt = Crds.topicOperation(client).resource(new KafkaTopicBuilder().withNewMetadata()
                .withName(NAME)
                .withNamespace(namespace(NAMESPACE))
                .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                .withPartitions(2)
                .withReplicas(1)
                .endSpec().build()).create();
        return kt;
    }

    private static String testName(TestInfo testInfo) {
        return testInfo.getTestMethod().map(m -> m.getName() + "() " + testInfo.getDisplayName().replaceAll("[\r\n]+", " ")).orElse("");
    }

    @BeforeAll
    public static void setupKubeCluster() {
        try {
            KubeCluster.bootstrap();
        } catch (NoClusterException e) {
            assumeTrue(false, e.getMessage());
        }
        KubeClusterResource.getInstance();
        cmdKubeClient().createNamespace(NAMESPACE);
        LOGGER.info("#### Creating " + "../packaging/install/topic-operator/02-Role-strimzi-topic-operator.yaml");
        cmdKubeClient().create(TestUtils.USER_PATH + "/../packaging/install/topic-operator/02-Role-strimzi-topic-operator.yaml");
        LOGGER.info("#### Creating " + TestUtils.CRD_TOPIC);
        cmdKubeClient().create(TestUtils.CRD_TOPIC);
        LOGGER.info("#### Creating " + TestUtils.USER_PATH + "/src/test/resources/TopicOperatorIT-rbac.yaml");
        cmdKubeClient().create(TestUtils.USER_PATH + "/src/test/resources/TopicOperatorIT-rbac.yaml");
    }

    @AfterAll
    public static void teardownKubeCluster() {
        cmdKubeClient()
                .delete(TestUtils.USER_PATH + "/src/test/resources/TopicOperatorIT-rbac.yaml")
                .delete(TestUtils.CRD_TOPIC)
                .delete(TestUtils.USER_PATH + "/../packaging/install/topic-operator/02-Role-strimzi-topic-operator.yaml")
                .deleteNamespace(NAMESPACE);
    }

    @AfterEach
    public void after(TestInfo testInfo) throws InterruptedException {
        LOGGER.debug("Cleaning up after test {}", testName(testInfo));
        if (admin[0] != null) {
            admin[0].close();
        }

        String pop = NAMESPACE;
        LOGGER.debug("Cleaning up namespace {} after test {}", pop, testName(testInfo));
        for (var kt : Crds.topicOperation(client).inNamespace(pop).list().getItems()) {
            LOGGER.debug("Removing finalizer on {} after test {}", kt.getMetadata().getName(), testName(testInfo));
            modifyTopic(kt, theKt -> {
                theKt.getMetadata().getFinalizers().clear();
                return theKt;
            });
        }
        for (var kt : Crds.topicOperation(client).inNamespace(pop).list().getItems()) {
            LOGGER.debug("Deleting KafkaTopic {} after test {}", kt.getMetadata().getName(), testName(testInfo));
            Crds.topicOperation(client).resource(kt).delete();
        }
        for (var kt : Crds.topicOperation(client).inNamespace(pop).list().getItems()) {
            waitUntilCondition(Crds.topicOperation(client).resource(kt), Objects::isNull);
        }

        client.close();
        LOGGER.debug("Cleaned up after test {}", testName(testInfo));
    }

    private KafkaTopic modifyTopic(KafkaTopic kt, UnaryOperator<KafkaTopic> changer) {
        String ns = kt.getMetadata().getNamespace();
        String metadataName = kt.getMetadata().getName();
        // Occasionally a single call to edit() will throw with a HTTP 409 (Conflict)
        // so let's try up to 3 times
        int i = 2;
        while (true) {
            try {
                KafkaTopic edited = Crds.topicOperation(client).inNamespace(ns).withName(metadataName).edit(changer);
                return edited;
            } catch (KubernetesClientException e) {
                if (i == 0 || e.getCode() != 409 /* conflict */) {
                    throw e;
                }
            }
            i--;
        }
    }


    private static <T> T waitUntilCondition(Resource<T> resource, Predicate<T> condition) {
        long timeoutMs = 30000L;
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            try {
                return resource.waitUntilCondition(condition, 100, TimeUnit.MILLISECONDS);
            } catch (KubernetesClientTimeoutException e) {
                if (System.currentTimeMillis() > deadline) {
                    fail("Timeout after " + timeoutMs + "ms waiting for " + condition +
                            ", current resource: " + e.getResourcesNotReady().get(0), e);
                    throw new IllegalStateException(); // never actually thrown, because fail() will throw
                }
            }
        }
    }

    private void assertOnUpdateThrowsInterruptedException(KubernetesClient client, Admin admin, KafkaTopic kt) throws ExecutionException, InterruptedException {
        controller = new BatchingTopicController(Map.of("key", "VALUE"), admin, client, true);
        List<ReconcilableTopic> batch = List.of(new ReconcilableTopic(new Reconciliation("test", "KafkaTopic", NAMESPACE, NAME), kt, BatchingTopicController.topicName(kt)));
        assertThrows(InterruptedException.class, () -> controller.onUpdate(batch));
    }


    @Test
    public void shouldHandleInterruptedExceptionFromDescribeTopics(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(DescribeTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).allTopicNames();
        Mockito.doReturn(Map.of(NAME, interruptedFuture())).when(result).topicNameValues();
        Mockito.doReturn(result).when(adminSpy).describeTopics(any(Collection.class));

        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromDescribeConfigs(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(DescribeConfigsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicConfigResource(), interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).describeConfigs(Mockito.argThat(a -> a.stream().anyMatch(x -> x.type() == ConfigResource.Type.TOPIC)));

        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    private static ConfigResource topicConfigResource() {
        return new ConfigResource(ConfigResource.Type.TOPIC, NAME);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromCreateTopics(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(CreateTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(NAME, interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).createTopics(any());

        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromIncrementalAlterConfigs(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        admin[0].createTopics(List.of(new NewTopic(NAME, 1, (short) 1).configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")))).all().get();
        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(AlterConfigsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicConfigResource(), interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).incrementalAlterConfigs(any());
        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromCreatePartitions(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        admin[0].createTopics(List.of(new NewTopic(NAME, 1, (short) 1))).all().get();

        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(CreatePartitionsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(NAME, interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).createPartitions(any());
        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromListReassignments(
            @BrokerCluster(numBrokers = 2)
            TestcontainersKafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        admin[0].createTopics(List.of(new NewTopic(NAME, 2, (short) 2))).all().get();

        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(ListPartitionReassignmentsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).reassignments();
        Mockito.doReturn(result).when(adminSpy).listPartitionReassignments(any(Set.class));
        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromDeleteTopics(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        admin[0].createTopics(List.of(new NewTopic(NAME, 1, (short) 1))).all().get();
        var kt = createResource();
        var withFinalizer = Crds.topicOperation(client).inNamespace(NAMESPACE).withName(NAME).edit(theKt -> {
            return new KafkaTopicBuilder(theKt).editOrNewMetadata().addToFinalizers(BatchingTopicController.FINALIZER).endMetadata().build();
        });
        Crds.topicOperation(client).inNamespace(NAMESPACE).withName(NAME).delete();
        var withDeletionTimestamp = Crds.topicOperation(client).inNamespace(NAMESPACE).withName(NAME).get();

        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(DeleteTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(NAME, interruptedFuture())).when(result).topicNameValues();
        Mockito.doReturn(result).when(adminSpy).deleteTopics(any(TopicCollection.TopicNameCollection.class));
        assertOnUpdateThrowsInterruptedException(client, adminSpy, withDeletionTimestamp);
    }

    // TODO kube client interrupted exceptions


}