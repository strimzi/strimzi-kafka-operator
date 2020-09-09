/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.ClientUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import static io.strimzi.test.TestUtils.toYamlString;

public abstract class KafkaClientsResource {
    private static final Logger LOGGER = LogManager.getLogger(KafkaClientsResource.class);

    protected String producerName;
    protected String consumerName;
    protected String bootstrapServer;
    protected String topicName;
    protected int messageCount;
    protected String additionalConfig;
    protected String consumerGroup;
    protected long delayMs;

    public abstract static class KafkaClientsBuilder<T extends KafkaClientsBuilder<T>> {
        private String producerName;
        private String consumerName;
        private String bootstrapServer;
        private String topicName;
        private int messageCount;
        private String additionalConfig;
        private String consumerGroup;
        private long delayMs;

        public T withProducerName(String producerName) {
            this.producerName = producerName;
            return self();
        }

        public T withConsumerName(String consumerName) {
            this.consumerName = consumerName;
            return self();
        }

        public T withBootstrapServer(String bootstrapServer) {
            this.bootstrapServer = bootstrapServer;
            return self();
        }

        public T withTopicName(String topicName) {
            this.topicName = topicName;
            return self();
        }

        public T withMessageCount(int messageCount) {
            this.messageCount = messageCount;
            return self();
        }

        public T withAdditionalConfig(String additionalConfig) {
            this.additionalConfig = additionalConfig;
            return self();
        }

        public T withConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
            return self();
        }

        public T withDelayMs(long delayMs) {
            this.delayMs = delayMs;
            return self();
        }

        protected abstract KafkaClientsResource build();

        protected abstract T self();
    }

    protected KafkaClientsResource(KafkaClientsBuilder<?> builder) {
        if (builder.topicName == null || builder.topicName.isEmpty()) throw new InvalidParameterException("Topic name is not set.");
        if (builder.producerName == null || builder.producerName.isEmpty()) throw new InvalidParameterException("Producer name is not set.");
        if (builder.consumerName == null || builder.consumerName.isEmpty()) throw new InvalidParameterException("Consumer name is not set.");
        if (builder.bootstrapServer == null || builder.bootstrapServer.isEmpty()) throw new InvalidParameterException("Bootstrap server is not set.");
        if (builder.messageCount <= 0) throw  new InvalidParameterException("Message count is less than 1");
        if (builder.consumerGroup == null || builder.consumerGroup.isEmpty()) {
            LOGGER.info("Consumer group were not specified going to create the random one.");
            builder.consumerGroup = ClientUtils.generateRandomConsumerGroup();
        }

        producerName = builder.producerName;
        consumerName = builder.consumerName;
        bootstrapServer = builder.bootstrapServer;
        topicName = builder.topicName;
        messageCount = builder.messageCount;
        additionalConfig = builder.additionalConfig;
        consumerGroup = builder.consumerGroup;
        delayMs = builder.delayMs;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public static DoneableDeployment deployKafkaClients(String kafkaClusterName) {
        return deployKafkaClients(false, kafkaClusterName, null);
    }

    public static DoneableDeployment deployKafkaClients(boolean tlsListener, String kafkaClientsName, KafkaUser... kafkaUsers) {
        return deployKafkaClients(tlsListener, kafkaClientsName, true, kafkaUsers);
    }

    public static DoneableDeployment deployKafkaClients(boolean tlsListener, String kafkaClientsName, boolean hostnameVerification, KafkaUser...kafkaUsers) {
        Map<String, String> label = Collections.singletonMap(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE);
        Deployment kafkaClient = new DeploymentBuilder()
            .withNewMetadata()
                .withName(kafkaClientsName)
                .withLabels(label)
            .endMetadata()
            .withNewSpec()
                .withNewSelector()
                    .addToMatchLabels("app", kafkaClientsName)
                    .addToMatchLabels(label)
                .endSelector()
                .withReplicas(1)
                .withNewTemplate()
                    .withNewMetadata()
                        .addToLabels("app", kafkaClientsName)
                        .addToLabels(label)
                    .endMetadata()
                    .withSpec(createClientSpec(tlsListener, kafkaClientsName, hostnameVerification, kafkaUsers))
                .endTemplate()
            .endSpec()
            .build();

        return KubernetesResource.deployNewDeployment(kafkaClient);
    }

    private static PodSpec createClientSpec(boolean tlsListener, String kafkaClientsName, boolean hostnameVerification, KafkaUser...kafkaUsers) {
        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();
        ContainerBuilder containerBuilder = new ContainerBuilder()
            .withName(kafkaClientsName)
            .withImage(Environment.TEST_CLIENT_IMAGE)
            .withCommand("sleep")
            .withArgs("infinity")
            .withImagePullPolicy(Environment.COMPONENTS_IMAGE_PULL_POLICY);

        String producerConfiguration = ProducerConfig.ACKS_CONFIG + "=all\n";
        String consumerConfiguration = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "=earliest\n";

        if (kafkaUsers == null) {
            containerBuilder.addNewEnv().withName("PRODUCER_CONFIGURATION").withValue(producerConfiguration).endEnv();
            containerBuilder.addNewEnv().withName("CONSUMER_CONFIGURATION").withValue(consumerConfiguration).endEnv();

        } else {
            for (KafkaUser kafkaUser : kafkaUsers) {
                String kafkaUserName = kafkaUser.getMetadata().getName();
                boolean tlsUser = kafkaUser.getSpec() != null && kafkaUser.getSpec().getAuthentication() instanceof KafkaUserTlsClientAuthentication;
                boolean scramShaUser = kafkaUser.getSpec() != null && kafkaUser.getSpec().getAuthentication() instanceof KafkaUserScramSha512ClientAuthentication;

                containerBuilder.addNewEnv().withName("PRODUCER_CONFIGURATION").withValue(producerConfiguration).endEnv();
                containerBuilder.addNewEnv().withName("CONSUMER_CONFIGURATION").withValue(consumerConfiguration).endEnv();

                String envVariablesSuffix = String.format("_%s", kafkaUserName.replace("-", "_"));
                containerBuilder.addNewEnv().withName("KAFKA_USER" + envVariablesSuffix).withValue(kafkaUserName).endEnv();

                if (tlsListener) {
                    if (scramShaUser) {
                        producerConfiguration += "security.protocol=SASL_SSL\n";
                        producerConfiguration += saslConfigs(kafkaUser);
                        consumerConfiguration += "security.protocol=SASL_SSL\n";
                        consumerConfiguration += saslConfigs(kafkaUser);
                    } else {
                        producerConfiguration += "security.protocol=SSL\n";
                        consumerConfiguration += "security.protocol=SSL\n";
                    }
                    producerConfiguration +=
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG + "=/tmp/" + kafkaUserName + "-truststore.p12\n" +
                            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG + "=pkcs12\n";
                    consumerConfiguration +=
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG + "=/tmp/" + kafkaUserName + "-truststore.p12\n" +
                            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG + "=pkcs12\n";
                } else {
                    if (scramShaUser) {
                        producerConfiguration += "security.protocol=SASL_PLAINTEXT\n";
                        producerConfiguration += saslConfigs(kafkaUser);
                        consumerConfiguration += "security.protocol=SASL_PLAINTEXT\n";
                        consumerConfiguration += saslConfigs(kafkaUser);
                    } else {
                        producerConfiguration += "security.protocol=PLAINTEXT\n";
                        consumerConfiguration += "security.protocol=PLAINTEXT\n";
                    }
                }

                if (tlsUser) {
                    producerConfiguration +=
                        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG + "=/tmp/" + kafkaUserName + "-keystore.p12\n" +
                            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG + "=pkcs12\n";
                    consumerConfiguration +=
                        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG + "=/tmp/" + kafkaUserName + "-keystore.p12\n" +
                            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG + "=pkcs12\n";

                    containerBuilder.addNewEnv().withName("PRODUCER_TLS" + envVariablesSuffix).withValue("TRUE").endEnv()
                        .addNewEnv().withName("CONSUMER_TLS" + envVariablesSuffix).withValue("TRUE").endEnv();

                    String userSecretVolumeName = "tls-cert-" + kafkaUserName;
                    String userSecretMountPoint = "/opt/kafka/user-secret-" + kafkaUserName;

                    containerBuilder.addNewVolumeMount()
                        .withName(userSecretVolumeName)
                        .withMountPath(userSecretMountPoint)
                        .endVolumeMount()
                        .addNewEnv().withName("USER_LOCATION" + envVariablesSuffix).withValue(userSecretMountPoint).endEnv();

                    podSpecBuilder.addNewVolume()
                        .withName(userSecretVolumeName)
                        .withNewSecret()
                        .withSecretName(kafkaUserName)
                        .endSecret()
                        .endVolume();
                }

                if (tlsListener) {
                    String clusterName = kafkaUser.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
                    String clusterNamespace = KafkaResource.kafkaClient().inAnyNamespace().list().getItems().stream().filter(kafka -> kafka.getMetadata().getName().equals(clusterName)).findFirst().get().getMetadata().getNamespace();
                    String clusterCaSecretName = KafkaResource.getKafkaTlsListenerCaCertName(clusterNamespace, clusterName);
                    String clusterCaSecretVolumeName = "ca-cert-" + kafkaUserName;
                    String caSecretMountPoint = "/opt/kafka/cluster-ca-" + kafkaUserName;

                    containerBuilder
                        .addNewVolumeMount()
                        .withName(clusterCaSecretVolumeName)
                        .withMountPath(caSecretMountPoint)
                        .endVolumeMount()
                        .addNewEnv().withName("PRODUCER_TLS" + envVariablesSuffix).withValue("TRUE").endEnv()
                        .addNewEnv().withName("CONSUMER_TLS" + envVariablesSuffix).withValue("TRUE").endEnv()
                        .addNewEnv().withName("CA_LOCATION" + envVariablesSuffix).withValue(caSecretMountPoint).endEnv()
                        .addNewEnv().withName("TRUSTSTORE_LOCATION" + envVariablesSuffix).withValue("/tmp/" + kafkaUserName + "-truststore.p12").endEnv();

                    if (tlsUser) {
                        containerBuilder.addNewEnv().withName("KEYSTORE_LOCATION" + envVariablesSuffix).withValue("/tmp/" + kafkaUserName + "-keystore.p12").endEnv();
                    }

                    podSpecBuilder
                        .addNewVolume()
                        .withName(clusterCaSecretVolumeName)
                        .withNewSecret()
                        .withSecretName(clusterCaSecretName)
                        .endSecret()
                        .endVolume();
                }

                if (!hostnameVerification) {
                    producerConfiguration += SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG + "=";
                    consumerConfiguration += SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG + "=";
                }


                containerBuilder.addNewEnv().withName("PRODUCER_CONFIGURATION" + envVariablesSuffix).withValue(producerConfiguration).endEnv();
                containerBuilder.addNewEnv().withName("CONSUMER_CONFIGURATION" + envVariablesSuffix).withValue(consumerConfiguration).endEnv();

                containerBuilder.withResources(new ResourceRequirementsBuilder()
                    .addToRequests("memory", new Quantity("200M"))
                    .build());
            }
        }
        return podSpecBuilder.withContainers(containerBuilder.build()).build();
    }

    static String saslConfigs(KafkaUser kafkaUser) {
        Secret secret = ResourceManager.kubeClient().getSecret(kafkaUser.getMetadata().getName());

        String password = new String(Base64.getDecoder().decode(secret.getData().get("password")), Charset.forName("UTF-8"));
        if (password.isEmpty()) {
            LOGGER.info("Secret {}:\n{}", kafkaUser.getMetadata().getName(), toYamlString(secret));
            throw new RuntimeException("The Secret " + kafkaUser.getMetadata().getName() + " lacks the 'password' key");
        }
        return "sasl.mechanism=SCRAM-SHA-512\n" +
            "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\\n" +
            "username=\"" + kafkaUser.getMetadata().getName() + "\" \\\n" +
            "password=\"" + password + "\";\n";
    }
}