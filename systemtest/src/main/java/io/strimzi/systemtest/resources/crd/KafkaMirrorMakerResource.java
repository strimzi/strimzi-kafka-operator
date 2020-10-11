/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

public class KafkaMirrorMakerResource {
    private static final Logger LOGGER = LogManager.getLogger(KafkaMirrorMakerResource.class);

    public static final String PATH_TO_KAFKA_MIRROR_MAKER_CONFIG = "../examples/kafka-mirror-maker/kafka-mirror-maker.yaml";

    public static MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker, Resource<KafkaMirrorMaker, DoneableKafkaMirrorMaker>> kafkaMirrorMakerClient() {
        return Crds.mirrorMakerOperation(ResourceManager.kubeClient().getClient());
    }

    public static DoneableKafkaMirrorMaker kafkaMirrorMaker(String name, String sourceBootstrapServer, String targetBootstrapServer, String groupId, int mirrorMakerReplicas, boolean tlsListener) {
        KafkaMirrorMaker kafkaMirrorMaker = getKafkaMirrorMakerFromYaml(PATH_TO_KAFKA_MIRROR_MAKER_CONFIG);
        return deployKafkaMirrorMaker(defaultKafkaMirrorMaker(kafkaMirrorMaker, name, sourceBootstrapServer, targetBootstrapServer, groupId, mirrorMakerReplicas, tlsListener).build());
    }

    public static KafkaMirrorMakerBuilder defaultKafkaMirrorMaker(String name,
                                                                  String sourceBootstrapServer,
                                                                  String targetBootstrapServer,
                                                                  String groupId,
                                                                  int kafkaMirrorMakerReplicas,
                                                                  boolean tlsListener) {
        KafkaMirrorMaker kafkaMirrorMaker = getKafkaMirrorMakerFromYaml(PATH_TO_KAFKA_MIRROR_MAKER_CONFIG);
        return defaultKafkaMirrorMaker(kafkaMirrorMaker, name, sourceBootstrapServer, targetBootstrapServer, groupId, kafkaMirrorMakerReplicas, tlsListener);
    }

    private static KafkaMirrorMakerBuilder defaultKafkaMirrorMaker(KafkaMirrorMaker kafkaMirrorMaker,
                                                                   String name,
                                                                   String sourceBootstrapServer,
                                                                   String targetBootstrapServer,
                                                                   String groupId,
                                                                   int kafkaMirrorMakerReplicas,
                                                                   boolean tlsListener) {
        return new KafkaMirrorMakerBuilder(kafkaMirrorMaker)
            .withNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
            .endMetadata()
            .editSpec()
                .withVersion(Environment.ST_KAFKA_VERSION)
                .withNewConsumer()
                    .withBootstrapServers(tlsListener ? KafkaResources.tlsBootstrapAddress(sourceBootstrapServer) : KafkaResources.plainBootstrapAddress(sourceBootstrapServer))
                    .withGroupId(groupId)
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
                .withNewProducer()
                    .withBootstrapServers(tlsListener ? KafkaResources.tlsBootstrapAddress(targetBootstrapServer) : KafkaResources.plainBootstrapAddress(targetBootstrapServer))
                    .addToConfig(ProducerConfig.ACKS_CONFIG, "all")
                .endProducer()
                .withReplicas(kafkaMirrorMakerReplicas)
                .withWhitelist(".*")
                .withNewInlineLogging()
                    .addToLoggers("mirrormaker.root.logger", "DEBUG")
                .endInlineLogging()
            .endSpec();
    }

    private static DoneableKafkaMirrorMaker deployKafkaMirrorMaker(KafkaMirrorMaker kafkaMirrorMaker) {
        return new DoneableKafkaMirrorMaker(kafkaMirrorMaker, kB -> {
            TestUtils.waitFor("KafkaMirrorMaker creation", Constants.POLL_INTERVAL_FOR_RESOURCE_CREATION, Constants.TIMEOUT_FOR_CR_CREATION,
                () -> {
                    try {
                        kafkaMirrorMakerClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kB);
                        return true;
                    } catch (KubernetesClientException e) {
                        if (e.getMessage().contains("object is being deleted")) {
                            return false;
                        } else {
                            throw e;
                        }
                    }
                }
            );
            return waitFor(deleteLater(kB));
        });
    }

    public static KafkaMirrorMaker kafkaMirrorMakerWithoutWait(KafkaMirrorMaker kafkaMirrorMaker) {
        kafkaMirrorMakerClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(kafkaMirrorMaker);
        return kafkaMirrorMaker;
    }

    public static void deleteKafkaMirrorMakerWithoutWait(KafkaMirrorMaker kafkaMirrorMaker) {
        kafkaMirrorMakerClient().inNamespace(ResourceManager.kubeClient().getNamespace()).delete(kafkaMirrorMaker);
    }

    private static KafkaMirrorMaker getKafkaMirrorMakerFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaMirrorMaker.class);
    }

    private static KafkaMirrorMaker waitFor(KafkaMirrorMaker kafkaMirrorMaker) {
        String kafkaMirrorMakerCrName = kafkaMirrorMaker.getMetadata().getName();

        LOGGER.info("Waiting for Kafka MirrorMaker {}", kafkaMirrorMakerCrName);
        DeploymentUtils.waitForDeploymentReady(KafkaMirrorMakerResources.deploymentName(kafkaMirrorMakerCrName), kafkaMirrorMaker.getSpec().getReplicas());
        LOGGER.info("Kafka MirrorMaker {} is ready", kafkaMirrorMakerCrName);

        return kafkaMirrorMaker;
    }

    private static KafkaMirrorMaker deleteLater(KafkaMirrorMaker kafkaMirrorMaker) {
        return ResourceManager.deleteLater(kafkaMirrorMakerClient(), kafkaMirrorMaker);
    }

    public static void replaceMirrorMakerResource(String resourceName, Consumer<KafkaMirrorMaker> editor) {
        ResourceManager.replaceCrdResource(KafkaMirrorMaker.class, KafkaMirrorMakerList.class, DoneableKafkaMirrorMaker.class, resourceName, editor);
    }
}
