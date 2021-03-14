/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.DeploymentTypes;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.test.TestUtils.toYamlString;

public class KafkaClientsTemplates {

    private static final Logger LOGGER = LogManager.getLogger(KafkaClientsResource.class);

    private KafkaClientsTemplates() { }

    public static DeploymentBuilder kafkaClients(String kafkaClusterName) {
        return kafkaClients(false, kafkaClusterName, null);
    }

    public static DeploymentBuilder kafkaClients(boolean tlsListener, String kafkaClientsName, KafkaUser... kafkaUsers) {
        return kafkaClients(tlsListener, kafkaClientsName, true,  null, null, kafkaUsers);
    }

    public static DeploymentBuilder kafkaClients(boolean tlsListener, String kafkaClientsName, String listenerName,
                                          KafkaUser... kafkaUsers) {
        return kafkaClients(tlsListener, kafkaClientsName, true, listenerName, null, kafkaUsers);
    }

    public static DeploymentBuilder kafkaClients(boolean tlsListener, String kafkaClientsName, boolean hostnameVerification,
                                          KafkaUser... kafkaUsers) {
        return kafkaClients(tlsListener, kafkaClientsName, hostnameVerification, null, null, kafkaUsers);
    }

    public static DeploymentBuilder kafkaClients(boolean tlsListener, String kafkaClientsName, boolean hostnameVerification,
                                          String listenerName, String secretPrefix, KafkaUser... kafkaUsers) {
        Map<String, String> label = new HashMap<>();

        label.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE);
        label.put(Constants.DEPLOYMENT_TYPE, DeploymentTypes.KafkaClients.name());

        DeploymentBuilder kafkaClient = new DeploymentBuilder()
            .withNewMetadata()
                .withName(kafkaClientsName)
                .withLabels(label)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
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
                    .withSpec(createClientSpec(tlsListener, kafkaClientsName, hostnameVerification, listenerName, secretPrefix, kafkaUsers))
                .endTemplate()
            .endSpec();

        return kafkaClient;
    }

    private static PodSpec createClientSpec(boolean tlsListener, String kafkaClientsName, boolean hostnameVerification,
                                            String listenerName, String secretPrefix, KafkaUser... kafkaUsers) {
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
                String kafkaUserName = secretPrefix == null ? kafkaUser.getMetadata().getName() : secretPrefix + kafkaUser.getMetadata().getName();
                boolean tlsUser = kafkaUser.getSpec() != null && kafkaUser.getSpec().getAuthentication() instanceof KafkaUserTlsClientAuthentication;
                boolean scramShaUser = kafkaUser.getSpec() != null && kafkaUser.getSpec().getAuthentication() instanceof KafkaUserScramSha512ClientAuthentication;

                containerBuilder.addNewEnv().withName("PRODUCER_CONFIGURATION").withValue(producerConfiguration).endEnv();
                containerBuilder.addNewEnv().withName("CONSUMER_CONFIGURATION").withValue(consumerConfiguration).endEnv();

                String envVariablesSuffix = String.format("_%s", kafkaUserName.replace("-", "_"));
                containerBuilder.addNewEnv().withName("KAFKA_USER" + envVariablesSuffix).withValue(kafkaUserName).endEnv();

                if (tlsListener) {
                    if (scramShaUser) {
                        producerConfiguration += "security.protocol=SASL_SSL\n";
                        producerConfiguration += saslConfigs(kafkaUser, secretPrefix);
                        consumerConfiguration += "security.protocol=SASL_SSL\n";
                        consumerConfiguration += saslConfigs(kafkaUser, secretPrefix);
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
                        producerConfiguration += saslConfigs(kafkaUser, secretPrefix);
                        consumerConfiguration += "security.protocol=SASL_PLAINTEXT\n";
                        consumerConfiguration += saslConfigs(kafkaUser, secretPrefix);
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

                    containerBuilder
                        .addNewVolumeMount()
                            .withName(userSecretVolumeName)
                            .withMountPath(userSecretMountPoint)
                        .endVolumeMount()
                        .addNewEnv()
                            .withName("USER_LOCATION" + envVariablesSuffix).withValue(userSecretMountPoint)
                        .endEnv();

                    podSpecBuilder
                        .addNewVolume()
                            .withName(userSecretVolumeName)
                            .withNewSecret()
                                .withSecretName(kafkaUserName)
                            .endSecret()
                        .endVolume();
                }

                if (tlsListener) {
                    String clusterName = kafkaUser.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
                    String clusterNamespace = KafkaResource.kafkaClient().inAnyNamespace().list().getItems().stream().filter(kafka -> kafka.getMetadata().getName().equals(clusterName)).findFirst().get().getMetadata().getNamespace();
                    String clusterCaSecretName = KafkaUtils.getKafkaTlsListenerCaCertName(clusterNamespace, clusterName, listenerName);
                    String clusterCaSecretVolumeName = "ca-cert-" + kafkaUserName;
                    String caSecretMountPoint = "/opt/kafka/cluster-ca-" + kafkaUserName;

                    containerBuilder
                        .addNewVolumeMount()
                            .withName(clusterCaSecretVolumeName)
                            .withMountPath(caSecretMountPoint)
                        .endVolumeMount()
                        .addNewEnv()
                            .withName("PRODUCER_TLS" + envVariablesSuffix).withValue("TRUE")
                        .endEnv()
                        .addNewEnv()
                            .withName("CONSUMER_TLS" + envVariablesSuffix).withValue("TRUE")
                        .endEnv()
                        .addNewEnv()
                            .withName("CA_LOCATION" + envVariablesSuffix).withValue(caSecretMountPoint)
                        .endEnv()
                        .addNewEnv()
                            .withName("TRUSTSTORE_LOCATION" + envVariablesSuffix).withValue("/tmp/" + kafkaUserName + "-truststore.p12")
                        .endEnv();

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

    static String saslConfigs(KafkaUser kafkaUser, String secretPrefix) {
        String secretName = secretPrefix == null ? kafkaUser.getMetadata().getName() : secretPrefix + kafkaUser.getMetadata().getName();
        Secret secret = ResourceManager.kubeClient().getSecret(secretName);

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
