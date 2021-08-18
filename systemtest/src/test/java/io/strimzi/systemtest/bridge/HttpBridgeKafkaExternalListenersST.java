/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.KafkaBridgeSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthentication;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBridgeExampleClients;
import io.strimzi.systemtest.resources.kubernetes.ServiceResource;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;

@Tag(REGRESSION)
@Tag(BRIDGE)
@Tag(NODEPORT_SUPPORTED)
@Tag(EXTERNAL_CLIENTS_USED)
@ParallelSuite
class HttpBridgeKafkaExternalListenersST extends HttpBridgeAbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeKafkaExternalListenersST.class);
    private static final String BRIDGE_EXTERNAL_SERVICE =  "shared-http-bridge-external-service";
    private static final String NAMESPACE = "bridge-kafka-external-listener-namespace";

    private final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
    private final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

    @ParallelTest
    void testScramShaAuthWithWeirdUsername(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        // Create weird named user with . and more than 64 chars -> SCRAM-SHA
        String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd.asdasidioiqweioqiweooioqieioqieoqieooi";

        // Initialize PasswordSecret to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecret = new PasswordSecretSource();
        passwordSecret.setSecretName(weirdUserName);
        passwordSecret.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(clusterName));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationScramSha512()
                .withUsername(weirdUserName)
                .withPasswordSecret(passwordSecret)
            .endKafkaClientAuthenticationScramSha512()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(extensionContext, weirdUserName, new KafkaListenerAuthenticationScramSha512(), bridgeSpec, SecurityProtocol.SASL_SSL);
    }

    @ParallelTest
    void testTlsAuthWithWeirdUsername(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        // Create weird named user with . and maximum of 64 chars -> TLS
        final String weirdUserName = "jjglmahyijoambryleyxjjglmahy.ijoambryleyxjjglmahyijoambryleyxasd";

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecret = new CertSecretSource();
        certSecret.setCertificate("ca.crt");
        certSecret.setSecretName(KafkaResources.clusterCaCertificateSecretName(clusterName));

        KafkaBridgeSpec bridgeSpec = new KafkaBridgeSpecBuilder()
            .withNewKafkaClientAuthenticationTls()
                .withNewCertificateAndKey()
                    .withSecretName(weirdUserName)
                    .withCertificate("user.crt")
                    .withKey("user.key")
                .endCertificateAndKey()
            .endKafkaClientAuthenticationTls()
            .withNewTls()
                .withTrustedCertificates(certSecret)
            .endTls()
            .build();

        testWeirdUsername(extensionContext, weirdUserName, new KafkaListenerAuthenticationTls(), bridgeSpec, SecurityProtocol.SSL);
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    private void testWeirdUsername(ExtensionContext extensionContext, String weirdUserName, KafkaListenerAuthentication auth, KafkaBridgeSpec spec, SecurityProtocol securityProtocol) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withAuth(auth)
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withAuth(auth)
                                .build())
                .endKafka()
            .endSpec().build());

        final KafkaBridgeExampleClients kafkaBridgeClientJob = (KafkaBridgeExampleClients) new KafkaBridgeExampleClients.Builder()
            .withBootstrapAddress(KafkaBridgeResources.serviceName(clusterName))
            .withProducerName(clusterName + "-" + producerName)
            .withConsumerName(clusterName + "-" + consumerName)
            .withTopicName(topicName)
            .withMessageCount(MESSAGE_COUNT)
            .withPort(bridgePort)
            .withDelayMs(1000)
            .withPollInterval(1000)
            .withNamespaceName(NAMESPACE)
            .build();

        // Create topic
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName)
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());

        // Create user
        if (auth.getType().equals(Constants.TLS_LISTENER_DEFAULT_NAME)) {
            resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterName, weirdUserName)
                .editMetadata()
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .build());
        } else {
            resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(clusterName, weirdUserName)
                .editMetadata()
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .build());
        }

        final String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(NAMESPACE, true, kafkaClientsName).build());

        // Deploy http bridge
        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridge(clusterName, KafkaResources.tlsBootstrapAddress(clusterName), 1)
                .editMetadata()
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpecLike(spec)
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(clusterName))
                    .withNewHttp(Constants.HTTP_BRIDGE_DEFAULT_PORT)
                .withNewConsumer()
                    .addToConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .endConsumer()
            .endSpec()
            .build());

        Service service = KafkaBridgeUtils.createBridgeNodePortService(clusterName, NAMESPACE, BRIDGE_EXTERNAL_SERVICE);
        ServiceResource.createServiceResource(extensionContext, service, NAMESPACE);

        resourceManager.createResource(extensionContext, kafkaBridgeClientJob.consumerStrimziBridge()
            .editMetadata()
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build());

        EnvVar clusterCaCert = new EnvVarBuilder()
            .withName("CA_CRT")
            .withNewValueFrom()
                .withNewSecretKeyRef()
                .withName(KafkaResources.clusterCaCertificateSecretName(clusterName))
                .withKey("ca.crt")
            .endSecretKeyRef()
            .endValueFrom()
            .build();

        List<EnvVar> envsVarsOfKafkaClient = new ArrayList<>();
        envsVarsOfKafkaClient.add(clusterCaCert);

        final String kafkaProducerExternalName = "kafka-producer-external" + new Random().nextInt(Integer.MAX_VALUE);
        String producerConfig = "";

        if (auth.getType().equals(Constants.TLS_LISTENER_DEFAULT_NAME)) {
            producerConfig =
                // tls
                "ssl.endpoint.identification.algorithm=\n" +
                "sasl.mechanism=GSSAPI\n" +
                "security.protocol=" + securityProtocol.name() + "\n";

            EnvVar userCrt = new EnvVarBuilder()
                .withName("USER_CRT")
                .withNewValueFrom()
                    .withNewSecretKeyRef()
                        .withName(weirdUserName)
                        .withKey("user.crt")
                    .endSecretKeyRef()
                .endValueFrom()
                .build();

            EnvVar userKey = new EnvVarBuilder()
                .withName("USER_KEY")
                .withNewValueFrom()
                    .withNewSecretKeyRef()
                        .withName(weirdUserName)
                        .withKey("user.key")
                    .endSecretKeyRef()
                .endValueFrom()
                .build();

            envsVarsOfKafkaClient.add(userCrt);
            envsVarsOfKafkaClient.add(userKey);
        } else {
            // fetch secret
            final String saslJaasConfigEncrypted = ResourceManager.kubeClient().getSecret(NAMESPACE, weirdUserName).getData().get("sasl.jaas.config");
            final String saslJaasConfigDecrypted = new String(Base64.getDecoder().decode(saslJaasConfigEncrypted), StandardCharsets.US_ASCII);

            producerConfig =
                // scram-sha
                "ssl.endpoint.identification.algorithm=\n" +
                "sasl.mechanism=SCRAM-SHA-512\n" +
                "security.protocol=" + securityProtocol.name() + "\n" +
                "sasl.jaas.config=" + saslJaasConfigDecrypted;
        }

        final List<ListenerStatus> listenerStatusList = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus().getListeners();
        final String externalBootstrapServers = listenerStatusList.stream().filter(listener -> listener.getType().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME))
            .findFirst()
            .orElseThrow(RuntimeException::new)
            .getBootstrapServers();

        final KafkaBasicExampleClients externalKafkaProducer = new KafkaBasicExampleClients.Builder()
            .withProducerName(kafkaProducerExternalName)
            .withBootstrapAddress(externalBootstrapServers)
            .withNamespaceName(NAMESPACE)
            .withTopicName(topicName)
            .withMessageCount(100)
            .withAdditionalConfig(producerConfig)
            .build();

        resourceManager.createResource(extensionContext, externalKafkaProducer.producerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addAllToEnv(envsVarsOfKafkaClient)
                    .endContainer()
                .endSpec()
            .endTemplate()
            .endSpec()
            .build());

        ClientUtils.waitForClientSuccess(kafkaProducerExternalName, NAMESPACE, 100);

        // delete kafka producer job
        JobUtils.deleteJobWithWait(NAMESPACE, kafkaProducerExternalName);

        ClientUtils.waitForClientSuccess(clusterName + "-" + consumerName, NAMESPACE, MESSAGE_COUNT);
    }

    @BeforeAll
    void createClassResources(ExtensionContext extensionContext) {
        LOGGER.debug("===============================================================");
        LOGGER.debug("{} - [BEFORE ALL] has been called", this.getClass().getName());

        cluster.createNamespace(extensionContext, NAMESPACE);
    }
}
