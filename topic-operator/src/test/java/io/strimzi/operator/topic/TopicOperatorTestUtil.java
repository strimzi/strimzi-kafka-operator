/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.junit.jupiter.api.Assertions.fail;

class TopicOperatorTestUtil {
    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorTestUtil.class);
    private TopicOperatorTestUtil() { }

    static String namespace(KubernetesClient client, String ns) {
        Resource<Namespace> resource = client.namespaces().withName(ns);
        if (resource.get() == null) {
            LOGGER.debug("Creating namespace {}", ns);
            client.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(ns).endMetadata().build()).create();
            waitUntilCondition(client.namespaces().withName(ns), Objects::nonNull);
        }
        return ns;
    }

    static String testName(TestInfo testInfo) {
        return testInfo.getTestMethod().map(m -> m.getName() + "() " + testInfo.getDisplayName().replaceAll("[\r\n]+", " ")).orElse("");
    }

    static void setupKubeCluster(TestInfo testInfo, String namespace) {
        KubeClusterResource.getInstance();
        try {
            cmdKubeClient().createNamespace(namespace);
        } catch (KubeClusterException.AlreadyExists e) {
            LOGGER.info("Namespace {} already exists, recreating", namespace);
            try (var tmpClient = TopicOperatorMain.kubeClient()) {
                cleanupNamespace(tmpClient, testInfo, namespace);
            }
            cmdKubeClient().deleteNamespace(namespace);
            cmdKubeClient().createNamespace(namespace);
        }
        LOGGER.info("Creating " + "../packaging/install/topic-operator/02-Role-strimzi-topic-operator.yaml");
        cmdKubeClient().create(TestUtils.USER_PATH + "/../packaging/install/topic-operator/02-Role-strimzi-topic-operator.yaml");
        LOGGER.info("Creating " + TestUtils.CRD_TOPIC);
        cmdKubeClient().create(TestUtils.CRD_TOPIC);
        LOGGER.info("Creating " + TestUtils.USER_PATH + "/src/test/resources/TopicOperatorIT-rbac.yaml");
        cmdKubeClient().create(TestUtils.USER_PATH + "/src/test/resources/TopicOperatorIT-rbac.yaml");
    }

    static void teardownKubeCluster(String namespace) {
        cmdKubeClient()
                .delete(TestUtils.USER_PATH + "/src/test/resources/TopicOperatorIT-rbac.yaml")
                .delete(TestUtils.CRD_TOPIC)
                .delete(TestUtils.USER_PATH + "/../packaging/install/topic-operator/02-Role-strimzi-topic-operator.yaml")
                .deleteNamespace(namespace);
    }

    static void cleanupNamespace(KubernetesClient client, TestInfo testInfo, String pop) {
        LOGGER.debug("Cleaning up namespace {} after test {}", pop, testName(testInfo));
        for (var kt : Crds.topicOperation(client).inNamespace(pop).list().getItems()) {
            LOGGER.debug("Removing finalizer on {} after test {}", kt.getMetadata().getName(), testName(testInfo));
            modifyTopic(client, kt, theKt -> {
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
    }

    static KafkaTopic modifyTopic(KubernetesClient client, KafkaTopic kt, UnaryOperator<KafkaTopic> changer) {
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

    static <T> T waitUntilCondition(Resource<T> resource, Predicate<T> condition) {
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

    static KafkaTopic findKafkaTopicByName(List<KafkaTopic> topics, String name) {
        return topics.stream().filter(kt -> kt.getMetadata().getName().equals(name)).findFirst().orElse(null);
    }
}
