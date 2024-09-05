/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.dsl.NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.fail;

public class TopicOperatorTestUtil {
    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorTestUtil.class);
    
    private TopicOperatorTestUtil() { }

    public static <T> String namespaceName(Class<T> clazz) {
        return clazz != null ? clazz.getSimpleName().toLowerCase(Locale.ROOT) : "strimzi-topic-operator";
    }

    public static void setupKubeCluster(KubernetesClient kubernetesClient, String namespace) {
        deleteNamespace(kubernetesClient, namespace);
        createNamespace(kubernetesClient, namespace);
        createOrReplace(kubernetesClient, "file://" + TestUtils.USER_PATH + "/../packaging/install/topic-operator/01-ServiceAccount-strimzi-topic-operator.yaml", namespace);
        createOrReplace(kubernetesClient, "file://" + TestUtils.USER_PATH + "/../packaging/install/topic-operator/02-Role-strimzi-topic-operator.yaml", namespace);
        createOrReplace(kubernetesClient, "file://" + TestUtils.USER_PATH + "/../packaging/install/topic-operator/03-RoleBinding-strimzi-topic-operator.yaml", namespace);
        createOrReplace(kubernetesClient, "file://" + TestUtils.CRD_TOPIC);
    }

    private static void createOrReplace(KubernetesClient kubernetesClient, String resourcesPath) {
        createOrReplace(kubernetesClient, resourcesPath, null);
    }

    private static void createOrReplace(KubernetesClient kubernetesClient, String resourcesPath, String namespace) {
        if (kubernetesClient == null || resourcesPath == null || resourcesPath.isBlank()) {
            throw new IllegalArgumentException();
        }
        NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata> loadedResources;
        try {
            loadedResources = kubernetesClient.load(new BufferedInputStream(new URL(resourcesPath).openStream()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (loadedResources != null && !loadedResources.items().isEmpty()) {
            try {
                LOGGER.info("Creating resources from {}", resourcesPath);
                if (namespace == null || namespace.isBlank()) {
                    loadedResources.create();
                } else {
                    loadedResources.inNamespace(namespace).create();
                }
            } catch (KubernetesClientException kce) {
                if (kce.getCode() != HttpURLConnection.HTTP_CONFLICT) {
                    throw kce;
                } else {
                    if (namespace == null || namespace.isBlank()) {
                        loadedResources.update();
                    } else {
                        loadedResources.inNamespace(namespace).update();
                    }
                }
            }
        }
    }

    public static void createNamespace(KubernetesClient kubernetesClient, String name) {
        Resource<Namespace> resource = kubernetesClient.namespaces().withName(name);
        if (resource.get() == null) {
            LOGGER.info("Creating namespace {}", name);
            kubernetesClient.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(name).endMetadata().build()).create();
            waitUntilCondition(kubernetesClient.namespaces().withName(name), Objects::nonNull);
        }
    }

    public static void deleteNamespace(KubernetesClient kubernetesClient, String name) {
        Resource<Namespace> resource = kubernetesClient.namespaces().withName(name);
        if (resource != null && resource.get() != null) {
            LOGGER.info("Deleting namespace {}", name);
            cleanupNamespace(kubernetesClient, name); // in case a previous test was interrupted leaving topics with finalizers
            kubernetesClient.namespaces().resource(resource.get()).delete();
            waitUntilCondition(kubernetesClient.namespaces().withName(name), Objects::isNull);
        }
    }

    public static void cleanupNamespace(KubernetesClient kubernetesClient, String name) {
        LOGGER.debug("Cleaning up namespace {}", name);
        try {
            var kafkaTopics = Crds.topicOperation(kubernetesClient).inNamespace(name).list();
            for (var kafkaTopic : kafkaTopics.getItems()) {
                LOGGER.debug("Removing finalizer on topic {}", kafkaTopic.getMetadata().getName());
                changeTopic(kubernetesClient, kafkaTopic, theKt -> {
                    theKt.getMetadata().getFinalizers().clear();
                    return theKt;
                });
            }
            for (var kafkaTopic : kafkaTopics.getItems()) {
                LOGGER.debug("Deleting topic {}", kafkaTopic.getMetadata().getName());
                Crds.topicOperation(kubernetesClient).resource(kafkaTopic).delete();
                waitUntilCondition(Crds.topicOperation(kubernetesClient).resource(kafkaTopic), Objects::isNull);
            }
        } catch (KubernetesClientException e) {
            if (e.getCode() != HttpURLConnection.HTTP_NOT_FOUND) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    public static ReconcilableTopic reconcilableTopic(KafkaTopic kafkaTopic, String namespace) {
        return new ReconcilableTopic(new Reconciliation("test", KafkaTopic.RESOURCE_KIND, namespace,
            TopicOperatorUtil.topicName(kafkaTopic)), kafkaTopic, TopicOperatorUtil.topicName(kafkaTopic));
    }

    public static KafkaTopic changeTopic(KubernetesClient client, KafkaTopic kt, UnaryOperator<KafkaTopic> changer) {
        String ns = kt.getMetadata().getNamespace();
        String metadataName = kt.getMetadata().getName();
        // occasionally a single call to edit() will throw with a HTTP 409 (Conflict) so let's try up to 3 times
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

    public static <T> T waitUntilCondition(Resource<T> resource, Predicate<T> condition) {
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

    public static KafkaTopic findKafkaTopicByName(List<KafkaTopic> topics, String name) {
        return topics.stream().filter(kt -> kt.getMetadata().getName().equals(name)).findFirst().orElse(null);
    }
    
    public static String contentFromTextFile(File filePath) {
        try {
            var resourceURI = Objects.requireNonNull(filePath).toURI();
            try (var lines = Files.lines(Paths.get(resourceURI), StandardCharsets.UTF_8)) {
                var content = lines.reduce((x, y) -> x + y);
                return content.orElseThrow(() -> new IOException(String.format("File %s is empty", filePath.getAbsolutePath())));
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
