/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security.custom;

import io.fabric8.kubernetes.api.model.ServiceAccountTokenProjectionBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.testclients.clients.kafka.KafkaProducerClient;
import io.strimzi.testclients.clients.kafka.KafkaProducerClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.REGRESSION;

/**
 * Test suite for the custom authentication mechanisms that are not covered by the dedicated authentication types.
 * Currently, it covers the authentication based on the Kubernetes Service Account tokens, which uses the `custom`
 * authentication together with the OAUTHBEARER SASL mechanism and the Kubernetes API server as the identity provider.
 */
@Tag(REGRESSION)
@Tag(CONNECT)
@Tag(CONNECT_COMPONENTS)
@SuiteDoc(
    description = @Desc("Test suite for verifying the custom authentication based on the Kubernetes Service Account tokens."),
    beforeTestSteps = {
        @Step(value = "Deploy the Cluster Operator.", expected = "Cluster Operator is deployed and ready.")
    },
    labels = {
        @Label(value = TestDocsLabels.SECURITY)
    }
)
public class CustomAuthenticationST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(CustomAuthenticationST.class);

    private static final String LISTENER_AUDIENCE = "my-internal-listener";
    private static final String TOKEN_VOLUME_NAME = "auth-token";
    private static final String TOKEN_MOUNT_PATH = "/mnt/auth-token";
    private static final String TOKEN_FILE_PATH = TOKEN_MOUNT_PATH + "/token";
    private static final String JWKS_ENDPOINT_URI = "https://kubernetes.default.svc.cluster.local/openid/v1/jwks";
    private static final String ISSUER_URI = "https://kubernetes.default.svc.cluster.local";
    private static final String SERVICE_ACCOUNT_DIR = "/var/run/secrets/kubernetes.io/serviceaccount";

    private static final String OAUTH_PRINCIPAL_BUILDER_CLASS = "io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder";
    private static final String OAUTH_SERVER_CALLBACK_HANDLER_CLASS = "io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler";
    private static final String OAUTH_CLIENT_CALLBACK_HANDLER_CLASS = "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler";
    private static final String OAUTH_BEARER_LOGIN_MODULE = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";
    private static final String STRING_CONVERTER_CLASS = "org.apache.kafka.connect.storage.StringConverter";

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test case verifies the authentication based on the Kubernetes Service Account tokens. " +
            "The Kafka listener validates the tokens against the JWKS endpoint of the Kubernetes API server and requires a custom audience. " +
            "Kafka Connect and the Kafka clients authenticate with tokens obtained through projected Service Account token volumes."),
        steps = {
            @Step(value = "Create the broker and controller KafkaNodePools.", expected = "KafkaNodePools are created."),
            @Step(value = "Deploy a Kafka cluster with a custom listener that authenticates clients with Kubernetes Service Account tokens and requires a custom audience.", expected = "Kafka cluster is deployed and ready."),
            @Step(value = "Create a KafkaTopic.", expected = "KafkaTopic is ready."),
            @Step(value = "Deploy Kafka Connect with custom authentication and a projected Service Account token volume mounted into the Connect container.", expected = "Kafka Connect connects to the Kafka cluster and becomes ready."),
            @Step(value = "Create a FileStreamSink KafkaConnector consuming from the KafkaTopic.", expected = "KafkaConnector is ready."),
            @Step(value = "Produce messages with a Kafka client using a projected Service Account token with the expected audience.", expected = "The producer finishes successfully."),
            @Step(value = "Check the file sink of the KafkaConnector.", expected = "All produced messages are present in the file sink, so the connector consumed them over the authenticated listener."),
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @SuppressWarnings("checkstyle:MethodLength")
    void testServiceAccountAuthentication() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );

        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editSpec()
                .editKafka()
                    // The Strimzi OAuth library requires its own principal builder
                    .addToConfig("principal.builder.class", OAUTH_PRINCIPAL_BUILDER_CLASS)
                    .withListeners(new GenericKafkaListenerBuilder()
                        .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                        .withNewKafkaListenerAuthenticationCustomAuth()
                            .withSasl(true)
                            .addToListenerConfig("sasl.enabled.mechanisms", "OAUTHBEARER")
                            .addToListenerConfig("oauthbearer.sasl.server.callback.handler.class", OAUTH_SERVER_CALLBACK_HANDLER_CLASS)
                            .addToListenerConfig("oauthbearer.connections.max.reauth.ms", 3600000)
                            .addToListenerConfig("oauthbearer.sasl.jaas.config", OAUTH_BEARER_LOGIN_MODULE + " required "
                                    + "unsecuredLoginStringClaim_sub=\"unused\" "
                                    + "oauth.check.access.token.type=\"false\" "
                                    + "oauth.custom.claim.check=\"@.aud anyof ['" + LISTENER_AUDIENCE + "']\" "
                                    + "oauth.valid.issuer.uri=\"" + ISSUER_URI + "\" "
                                    + "oauth.jwks.endpoint.uri=\"" + JWKS_ENDPOINT_URI + "\" "
                                    + "oauth.jwks.refresh.seconds=\"300\" "
                                    + "oauth.username.claim=\"sub\" "
                                    + "oauth.ssl.truststore.location=\"" + SERVICE_ACCOUNT_DIR + "/ca.crt\" "
                                    + "oauth.ssl.truststore.type=\"PEM\" "
                                    + "oauth.server.bearer.token.location=\"" + SERVICE_ACCOUNT_DIR + "/token\" "
                                    + "oauth.include.accept.header=\"false\";")
                        .endKafkaListenerAuthenticationCustomAuth()
                        .build())
                .endKafka()
            .endSpec()
            .build());

        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        final KafkaConnect connect = KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .editSpec()
                .withBootstrapServers(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
                .withNewKafkaClientAuthenticationCustom()
                    .withSasl(true)
                    .addToConfig("sasl.mechanism", "OAUTHBEARER")
                    .addToConfig("sasl.login.callback.handler.class", OAUTH_CLIENT_CALLBACK_HANDLER_CLASS)
                    .addToConfig("sasl.jaas.config", OAUTH_BEARER_LOGIN_MODULE + " required oauth.access.token.location=\"" + TOKEN_FILE_PATH + "\";")
                .endKafkaClientAuthenticationCustom()
                .addToConfig("key.converter", STRING_CONVERTER_CLASS)
                .addToConfig("value.converter", STRING_CONVERTER_CLASS)
                .addToConfig("key.converter.schemas.enable", false)
                .addToConfig("value.converter.schemas.enable", false)
                .editOrNewTemplate()
                    .editOrNewPod()
                        .addNewVolume()
                            .withName(TOKEN_VOLUME_NAME)
                            .withNewProjected()
                                .addNewSource()
                                    .withServiceAccountToken(new ServiceAccountTokenProjectionBuilder()
                                            .withAudience(LISTENER_AUDIENCE)
                                            .withExpirationSeconds(3600L)
                                            .withPath("token")
                                            .build())
                                .endSource()
                            .endProjected()
                        .endVolume()
                    .endPod()
                    .editOrNewConnectContainer()
                        .addToVolumeMounts(new VolumeMountBuilder()
                            .withName(TOKEN_VOLUME_NAME)
                            .withMountPath(TOKEN_MOUNT_PATH)
                            .build())
                    .endConnectContainer()
                .endTemplate()
            .endSpec()
            .build();

        // The TLS configuration comes from the default template and cannot be removed through the builder
        connect.getSpec().setTls(null);

        KubeResourceManager.get().createResourceWithWait(connect);

        final String connectPodName = KubeResourceManager.get().kubeClient()
            .listPodsByPrefixInName(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()))
            .get(0).getMetadata().getName();

        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(testStorage.getNamespaceName(), connectPodName);

        LOGGER.info("Creating FileStreamSink KafkaConnector: {}/{} for Topic: {}", testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getTopicName());
        KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), testStorage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", testStorage.getTopicName())
                .addToConfig("file", TestConstants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", STRING_CONVERTER_CLASS)
                .addToConfig("value.converter", STRING_CONVERTER_CLASS)
            .endSpec()
            .build());

        LOGGER.info("Producing messages with a Service Account token issued for the expected audience: {}", LISTENER_AUDIENCE);
        final KafkaProducerClient producer = new KafkaProducerClientBuilder()
                .withName(testStorage.getProducerName())
                .withNamespaceName(testStorage.getNamespaceName())
                .withTopicName(testStorage.getTopicName())
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
                .withMessageCount(testStorage.getMessageCount())
                .withAdditionalConfig(
                        "security.protocol=SASL_PLAINTEXT\n"
                                + "sasl.mechanism=OAUTHBEARER\n"
                                + "sasl.login.callback.handler.class=" + OAUTH_CLIENT_CALLBACK_HANDLER_CLASS + "\n"
                                + "sasl.jaas.config=" + OAUTH_BEARER_LOGIN_MODULE + " required oauth.access.token.location=\"" + TOKEN_FILE_PATH + "\";"
                )
                .build();

        // We update the producer job to have the projected token as well
        final Job producerJob = new JobBuilder(producer.getJob())
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .addNewVolume()
                            .withName(TOKEN_VOLUME_NAME)
                            .withNewProjected()
                                .addNewSource()
                                    .withServiceAccountToken(new ServiceAccountTokenProjectionBuilder()
                                            .withAudience(LISTENER_AUDIENCE)
                                            .withExpirationSeconds(3600L)
                                            .withPath("token")
                                            .build())
                                .endSource()
                            .endProjected()
                        .endVolume()
                        .editFirstContainer()
                            .addNewVolumeMount()
                                .withName(TOKEN_VOLUME_NAME)
                                .withMountPath(TOKEN_MOUNT_PATH)
                            .endVolumeMount()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
        KubeResourceManager.get().createResourceWithWait(producerJob);
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());

        LOGGER.info("Checking that KafkaConnector consumed the messages over the Service Account authenticated listener");
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), connectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, testStorage.getMessageCount());
    }

    @BeforeAll
    void setUp() {
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();
    }
}
