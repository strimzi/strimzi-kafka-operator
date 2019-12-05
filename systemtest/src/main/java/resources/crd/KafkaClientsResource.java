/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package resources.crd;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.systemtest.Environment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import resources.KubernetesResource;
import resources.ResourceManager;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.test.TestUtils.toYamlString;

public class KafkaClientsResource {
    private static final Logger LOGGER = LogManager.getLogger(KafkaClientsResource.class);

    public static DoneableDeployment deployKafkaClients(String kafkaClientsName) {
        return deployKafkaClients(false, kafkaClientsName, null);
    }

    public static DoneableDeployment deployKafkaClients(boolean tlsListener, String kafkaClientsName) {
        return deployKafkaClients(tlsListener, kafkaClientsName, null);
    }

    public static DoneableDeployment deployKafkaClients(boolean tlsListener, String kafkaClientsName, KafkaUser... kafkaUsers) {
        Deployment kafkaClient = new DeploymentBuilder()
            .withNewMetadata()
                .withName(kafkaClientsName)
            .endMetadata()
            .withNewSpec()
                .withNewSelector()
                    .addToMatchLabels("app", kafkaClientsName)
                .endSelector()
                .withReplicas(1)
                .withNewTemplate()
                    .withNewMetadata()
                        .addToLabels("app", kafkaClientsName)
                    .endMetadata()
                    .withSpec(createClientSpec(tlsListener, kafkaClientsName, kafkaUsers))
                .endTemplate()
            .endSpec()
            .build();

        return KubernetesResource.deployNewDeployment(kafkaClient);
    }

    private static PodSpec createClientSpec(boolean tlsListener, String kafkaClientsName, KafkaUser... kafkaUsers) {
        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();
        ContainerBuilder containerBuilder = new ContainerBuilder()
            .withName(kafkaClientsName)
            .withImage(Environment.TEST_CLIENT_IMAGE)
            .withCommand("sleep")
            .withArgs("infinity")
            .withImagePullPolicy(Environment.IMAGE_PULL_POLICY);

        if (kafkaUsers == null) {
            String producerConfiguration = "acks=all\n";
            String consumerConfiguration = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "=earliest\n";

            containerBuilder.addNewEnv().withName("PRODUCER_CONFIGURATION").withValue(producerConfiguration).endEnv();
            containerBuilder.addNewEnv().withName("CONSUMER_CONFIGURATION").withValue(consumerConfiguration).endEnv();

        } else {
            for (KafkaUser kafkaUser : kafkaUsers) {
                String kafkaUserName = kafkaUser.getMetadata().getName();
                boolean tlsUser = kafkaUser.getSpec() != null && kafkaUser.getSpec().getAuthentication() instanceof KafkaUserTlsClientAuthentication;
                boolean scramShaUser = kafkaUser.getSpec() != null && kafkaUser.getSpec().getAuthentication() instanceof KafkaUserScramSha512ClientAuthentication;

                String producerConfiguration = "acks=all\n";
                String consumerConfiguration = "auto.offset.reset=earliest\n";
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
                            "ssl.truststore.location=/tmp/" + kafkaUserName + "-truststore.p12\n" +
                                    "ssl.truststore.type=pkcs12\n";
                    consumerConfiguration += ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "=earliest\n" +
                            "ssl.truststore.location=/tmp/" + kafkaUserName + "-truststore.p12\n" +
                            "ssl.truststore.type=pkcs12\n";
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
                            "ssl.keystore.location=/tmp/" + kafkaUserName + "-keystore.p12\n" +
                                    "ssl.keystore.type=pkcs12\n";
                    consumerConfiguration += "auto.offset.reset=earliest\n" +
                            "ssl.keystore.location=/tmp/" + kafkaUserName + "-keystore.p12\n" +
                            "ssl.keystore.type=pkcs12\n";

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
                    String clusterName = kafkaUser.getMetadata().getLabels().get("strimzi.io/cluster");
                    String clusterCaSecretName = clusterCaCertSecretName(clusterName);
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
                        .addNewEnv().withName("TRUSTSTORE_LOCATION" + envVariablesSuffix).withValue("/tmp/"  + kafkaUserName + "-truststore.p12").endEnv();

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

                containerBuilder.addNewEnv().withName("PRODUCER_CONFIGURATION" + envVariablesSuffix).withValue(producerConfiguration).endEnv();
                containerBuilder.addNewEnv().withName("CONSUMER_CONFIGURATION"  + envVariablesSuffix).withValue(consumerConfiguration).endEnv();
            }
        }
        return podSpecBuilder.withContainers(containerBuilder.build()).build();
    }


    static String clusterCaCertSecretName(String cluster) {
        return cluster + "-cluster-ca-cert";
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

    public static DoneableDeployment consumerWithTracing(String bootstrapServer) {
        String consumerName = "hello-world-consumer";

        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", consumerName);

        return KubernetesResource.deployNewDeployment(new DeploymentBuilder()
                    .withNewMetadata()
                        .withNamespace(ResourceManager.kubeClient().getNamespace())
                        .withLabels(consumerLabels)
                        .withName(consumerName)
                    .endMetadata()
                    .withNewSpec()
                        .withNewSelector()
                            .withMatchLabels(consumerLabels)
                        .endSelector()
                        .withReplicas(1)
                        .withNewTemplate()
                            .withNewMetadata()
                                .withLabels(consumerLabels)
                            .endMetadata()
                            .withNewSpec()
                                .withContainers()
                                .addNewContainer()
                                    .withName(consumerName)
                                    .withImage("strimzi/" + consumerName + ":latest")
                                    .addNewEnv()
                                        .withName("BOOTSTRAP_SERVERS")
                                        .withValue(bootstrapServer)
                                      .endEnv()
                                    .addNewEnv()
                                        .withName("TOPIC")
                                        .withValue("my-topic")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("GROUP_ID")
                                        .withValue("my-" + consumerName)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("DELAY_MS")
                                        .withValue("1000")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("LOG_LEVEL")
                                        .withValue("INFO")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("MESSAGE_COUNT")
                                        .withValue("1000000")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SERVICE_NAME")
                                        .withValue(consumerName)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_AGENT_HOST")
                                        .withValue("my-jaeger-agent")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SAMPLER_TYPE")
                                        .withValue("const")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SAMPLER_PARAM")
                                        .withValue("1")
                                    .endEnv()
                                .endContainer()
                            .endSpec()
                        .endTemplate()
                    .endSpec()
                    .build());
    }

    public static DoneableDeployment producerWithTracing(String bootstrapServer) {
        String producerName = "hello-world-producer";

        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", producerName);

        return KubernetesResource.deployNewDeployment(new DeploymentBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withLabels(producerLabels)
                .withName(producerName)
            .endMetadata()
            .withNewSpec()
                .withNewSelector()
                    .withMatchLabels(producerLabels)
                .endSelector()
                .withReplicas(1)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(producerLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withContainers()
                        .addNewContainer()
                            .withName(producerName)
                            .withImage("strimzi/" + producerName + ":latest")
                            .addNewEnv()
                                .withName("BOOTSTRAP_SERVERS")
                                .withValue(bootstrapServer)
                              .endEnv()
                            .addNewEnv()
                                .withName("TOPIC")
                                .withValue("my-topic")
                            .endEnv()
                            .addNewEnv()
                                .withName("DELAY_MS")
                                .withValue("1000")
                            .endEnv()
                            .addNewEnv()
                                .withName("LOG_LEVEL")
                                .withValue("INFO")
                            .endEnv()
                            .addNewEnv()
                                .withName("MESSAGE_COUNT")
                                .withValue("1000000")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(producerName)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue("my-jaeger-agent")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue("const")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue("1")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());
    }

    public static DoneableDeployment kafkaStreamsWithTracing(String bootstrapServer) {
        String kafkaStreamsName = "hello-world-streams";

        Map<String, String> kafkaStreamLabels = new HashMap<>();
        kafkaStreamLabels.put("app", kafkaStreamsName);

        return KubernetesResource.deployNewDeployment(new DeploymentBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withLabels(kafkaStreamLabels)
                .withName(kafkaStreamsName)
            .endMetadata()
            .withNewSpec()
                .withNewSelector()
                    .withMatchLabels(kafkaStreamLabels)
                .endSelector()
                .withReplicas(1)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(kafkaStreamLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withContainers()
                        .addNewContainer()
                            .withName(kafkaStreamsName)
                            .withImage("strimzi/" + kafkaStreamsName + ":latest")
                            .addNewEnv()
                                .withName("BOOTSTRAP_SERVERS")
                                .withValue(bootstrapServer)
                              .endEnv()
                            .addNewEnv()
                                .withName("APPLICATION_ID")
                                .withValue(kafkaStreamsName)
                            .endEnv()
                            .addNewEnv()
                                .withName("SOURCE_TOPIC")
                                .withValue("my-topic")
                            .endEnv()
                            .addNewEnv()
                                .withName("TARGET_TOPIC")
                                .withValue("cipot-ym")
                            .endEnv()
                              .addNewEnv()
                                .withName("LOG_LEVEL")
                                .withValue("INFO")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(kafkaStreamsName)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue("my-jaeger-agent")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue("const")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue("1")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());
    }

    public static DoneableDeployment deployKeycloak() {
        String keycloakName = "keycloak";

        Map<String, String> keycloakLabels = new HashMap<>();
        keycloakLabels.put("app", keycloakName);

        return KubernetesResource.deployNewDeployment(new DeploymentBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withLabels(keycloakLabels)
                .withName(keycloakName)
            .endMetadata()
            .withNewSpec()
                .withNewSelector()
                    .withMatchLabels(keycloakLabels)
                .endSelector()
                .withReplicas(1)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(keycloakLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withContainers()
                        .addNewContainer()
                            .withName(keycloakName + "pod")
                            .withImage("jboss/keycloak")
                            .withPorts(
                                new ContainerPortBuilder()
                                    .withName("http")
                                    .withContainerPort(8080)
                                    .build(),
                                new ContainerPortBuilder()
                                    .withName("https")
                                    .withContainerPort(8443)
                                    .build()
                            )
                            .addNewEnv()
                                .withName("KEYCLOAK_USER")
                                .withValue("admin")
                            .endEnv()
                            .addNewEnv()
                                .withName("KEYCLOAK_PASSWORD")
                                .withValue("admin")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());
    }

    public static DoneableDeployment producerWithOauth(String oauthTokenEndpointUri, String topicName, String bootstrap) {
        String producerName = "hello-world-producer";

        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", producerName);

        return KubernetesResource.deployNewDeployment(new DeploymentBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withClusterName(ResourceManager.kubeClient().getNamespace())
                .withLabels(producerLabels)
                .withName("hello-world-producer")
            .endMetadata()
            .withNewSpec()
                .withReplicas(1)
                .withNewSelector()
                    .withMatchLabels(producerLabels)
                .endSelector()
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(producerLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withContainers()
                        .addNewContainer()
                            .withName(producerName)
                            .withImage("strimzi/" + producerName + ":latest")
                            .addNewEnv()
                                .withName("BOOTSTRAP_SERVERS")
                                .withValue(bootstrap)
                            .endEnv()
                            .addNewEnv()
                                .withName("TOPIC")
                                .withValue(topicName)
                            .endEnv()
                            .addNewEnv()
                                .withName("DELAY_MS")
                                .withValue("1000")
                            .endEnv()
                            .addNewEnv()
                                .withName("LOG_LEVEL")
                                .withValue("DEBUG")
                            .endEnv()
                            .addNewEnv()
                                .withName("MESSAGE_COUNT")
                                .withValue("100")
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_ID")
                                .withValue("hello-world-producer")
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_SECRET")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName("hello-world-producer-oauth")
                                        .withKey("clientSecret")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_TOKEN_ENDPOINT_URI")
                                .withValue(oauthTokenEndpointUri)
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CRT")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName("x509-https-secret")
                                        .withKey("tls.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM") // disable hostname verification
                                .withValue("")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec().build());
    }


    public static DoneableDeployment consumerWithOauth(String oauthTokenEndpointUri, String topicName, String bootstrap) {
        return consumerWithOauth("hello-world-consumer", oauthTokenEndpointUri, topicName, bootstrap);
    }

    public static DoneableDeployment consumerWithOauth(String name, String oauthTokenEndpointUri, String topicName, String bootstrap) {
        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", name);

        return KubernetesResource.deployNewDeployment(new DeploymentBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withClusterName(ResourceManager.kubeClient().getNamespace())
                .withLabels(consumerLabels)
                .withName(name)
            .endMetadata()
            .withNewSpec()
                .withReplicas(1)
                .withNewSelector()
                    .withMatchLabels(consumerLabels)
                .endSelector()
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(consumerLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withContainers()
                        .addNewContainer()
                            .withName(name)
                            .withImage("strimzi/hello-world-consumer:latest")
                            .addNewEnv()
                                .withName("BOOTSTRAP_SERVERS")
                                .withValue(bootstrap)
                            .endEnv()
                            .addNewEnv()
                                .withName("TOPIC")
                                .withValue(topicName)
                            .endEnv()
                            .addNewEnv()
                                .withName("GROUP_ID")
                                .withValue(name)
                            .endEnv()
                            .addNewEnv()
                                .withName("LOG_LEVEL")
                                .withValue("INFO")
                            .endEnv()
                            .addNewEnv()
                                .withName("MESSAGE_COUNT")
                                .withValue("100")
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_ID")
                                .withValue("hello-world-consumer")
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_SECRET")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName("hello-world-consumer-oauth")
                                        .withKey("clientSecret")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_TOKEN_ENDPOINT_URI")
                                .withValue(oauthTokenEndpointUri)
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CRT")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName("x509-https-secret")
                                        .withKey("tls.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM")  // disable hostname verification
                                .withValue("")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec().build());
    }

    public static DoneableDeployment kafkaStreamsWithOauth(String oauthTokenEndpointUri, String bootstrap) {
        String kafkaStreamsName = "hello-world-streams";

        Map<String, String> kafkaStreamsLabel = new HashMap<>();
        kafkaStreamsLabel.put("app", kafkaStreamsName);

        return KubernetesResource.deployNewDeployment(new DeploymentBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withClusterName(ResourceManager.kubeClient().getNamespace())
                .withLabels(kafkaStreamsLabel)
                .withName(kafkaStreamsName)
            .endMetadata()
            .withNewSpec()
                .withReplicas(1)
                .withNewSelector()
                    .withMatchLabels(kafkaStreamsLabel)
                .endSelector()
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(kafkaStreamsLabel)
                    .endMetadata()
                    .withNewSpec()
                    .withContainers()
                        .addNewContainer()
                            .withName(kafkaStreamsName)
                            .withImage("strimzi/" + kafkaStreamsName + ":latest")
                            .addNewEnv()
                                .withName("BOOTSTRAP_SERVERS")
                                .withValue(bootstrap)
                            .endEnv()
                            .addNewEnv()
                                .withName("APPLICATION_ID")
                                .withValue(kafkaStreamsName)
                            .endEnv()
                            .addNewEnv()
                                .withName("SOURCE_TOPIC")
                                .withValue("my-topic")
                            .endEnv()
                            .addNewEnv()
                                .withName("TARGET_TOPIC")
                                .withValue("my-topic-reversed")
                            .endEnv()
                            .addNewEnv()
                                .withName("LOG_LEVEL")
                                .withValue("DEBUG")
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_ID")
                                .withValue(kafkaStreamsName)
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CLIENT_SECRET")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName("hello-world-streams-oauth")
                                        .withKey("clientSecret")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_TOKEN_ENDPOINT_URI")
                                .withValue(oauthTokenEndpointUri)
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_CRT")
                                .withNewValueFrom()
                                    .withNewSecretKeyRef()
                                        .withName("x509-https-secret")
                                        .withKey("tls.crt")
                                    .endSecretKeyRef()
                                .endValueFrom()
                            .endEnv()
                            .addNewEnv()
                                .withName("OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM") // disable hostname verification
                                .withValue("")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec().build());
    }
}
