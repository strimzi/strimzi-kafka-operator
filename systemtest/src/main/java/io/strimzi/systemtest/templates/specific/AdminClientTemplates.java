/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.specific;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Util;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.DeploymentTypes;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class AdminClientTemplates {

    private AdminClientTemplates() {}

    ///////////////////////////////////////////
    //              TLS (SSL)
    ///////////////////////////////////////////

    /**
     * Deploys an AdminClient with TLS enabled, with only essential configuration.
     *
     * @param namespaceName The Namespace in which to deploy the AdminClient and where Kafka resides.
     * @param userName The Kafka userName to correctly configure TLS.
     * @param adminName The name of the AdminClient deployment.
     * @param clusterName The name of the Kafka cluster to which the AdminClient will connect.
     * @param bootstrapName The name of the Kafka bootstrap server to use for the initial connection.
     */
    public static Deployment tlsAdminClient(String namespaceName, String userName, String adminName, String clusterName, String bootstrapName) {
        return tlsAdminClient(namespaceName, userName, adminName, clusterName, bootstrapName, "");
    }

    /**
     * Creates a Deployment configuration for an AdminClient with TLS settings.
     */
    public static Deployment tlsAdminClient(String namespaceName, String userName, String adminName, String clusterName, String bootstrapName, String additionalConfig) {
        final List<EnvVar> tlsEnvs = buildTLSUserCredentials(userName);
        final String finalAdditionalConfig = "sasl.mechanism=GSSAPI\n" + "security.protocol=" + SecurityProtocol.SSL + "\n"  + "\n" + additionalConfig;

        return defaultAdminClient(namespaceName, adminName, bootstrapName, finalAdditionalConfig)
            .editOrNewSpec()
                .editOrNewTemplate()
                    .editOrNewSpec()
                        .editFirstContainer()
                            .addToEnv(getClusterCaCertEnv(clusterName))
                            .addAllToEnv(tlsEnvs)
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }

    ///////////////////////////////////////////
    //    SCRAM-SHA over TLS (SASL_SSL)
    ///////////////////////////////////////////

    /**
     * Deploys an AdminClient with SCRAM-SHA and TLS enabled and has only essential configuration.
     *
     * @param namespaceName The Namespace in which to deploy the AdminClient and where Kafka resides.
     * @param userName The Kafka userName to correctly configure SCRAM-SHA.
     * @param adminName The name of the AdminClient deployment.
     * @param clusterName The name of the Kafka cluster to which the AdminClient will connect.
     * @param bootstrapName The name of the Kafka bootstrap server to use for the initial connection.
     */
    public static Deployment scramShaOverTlsAdminClient(String namespaceName, String userName, String adminName, String clusterName, String bootstrapName) {
        return scramShaOverTlsAdminClient(namespaceName, userName, adminName, clusterName, bootstrapName, "");
    }

    /**
     * Creates a Deployment configuration for an AdminClient with TLS and SCRAM_SHA settings.
     */
    public static Deployment scramShaOverTlsAdminClient(String namespaceName, String userName, String adminName, String clusterName, String bootstrapName, String additionalConfig) {
        String finalAdditionalConfig = getAdminClientScramConfig(namespaceName, userName, SecurityProtocol.SASL_SSL) + "\n" + additionalConfig;
        // authenticating is taken care of (by SASL) thus only cluster needed
        return defaultAdminClient(namespaceName, adminName, bootstrapName, finalAdditionalConfig)
            .editOrNewSpec()
                .editOrNewTemplate()
                    .editOrNewSpec()
                        .editFirstContainer()
                            .addToEnv(getClusterCaCertEnv(clusterName))
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }

    ///////////////////////////////////////////
    // SCRAM-SHA over Plain (SASL_PLAINTEXT)
    ///////////////////////////////////////////

    /**
     * Deploys an AdminClient with SCRAM-SHA over PLAINTEXT and has only essential configuration.
     *
     * @param namespaceName The Namespace in which to deploy the AdminClient and where Kafka resides.
     * @param userName The Kafka userName to correctly configure SCRAM-SHA.
     * @param adminName The name of the AdminClient deployment.
     * @param bootstrapName The name of the Kafka bootstrap server to use for the initial connection.
     */
    public static Deployment scramShaOverPlainAdminClient(String namespaceName, String userName, String adminName, String bootstrapName) {
        return scramShaOverPlainAdminClient(namespaceName, userName, adminName, bootstrapName, "");
    }

    /**
     * Creates a Deployment configuration for an AdminClient with PLAINTEXT and SCRAM_SHA settings.
     */
    public static Deployment scramShaOverPlainAdminClient(String namespaceName, String userName, String adminName, String bootstrapName, String additionalConfig) {
        String finalAdditionalConfig = getAdminClientScramConfig(namespaceName, userName, SecurityProtocol.SASL_PLAINTEXT) + "\n" + additionalConfig;
        return defaultAdminClient(namespaceName, adminName, bootstrapName, finalAdditionalConfig)
            .build();
    }

    ///////////////////////////////////////////
    //          plain (PLAINTEXT)
    ///////////////////////////////////////////

    /**
     * Deploys an AdminClient with PLAINTEXT communication and no other configuration.
     *
     * @param namespaceName The Namespace in which to deploy the AdminClient and where Kafka resides.
     * @param adminName The name of the AdminClient deployment.
     * @param bootstrapName The name of the Kafka bootstrap server to use for the initial connection.
     */
    public static DeploymentBuilder plainAdminClient(String namespaceName, String adminName, String bootstrapName) {
        return defaultAdminClient(namespaceName, adminName, bootstrapName, "");
    }

    /**
     * Creates a Deployment configuration for an AdminClient with PLAINTEXT communication and desired additional configuration.
     */
    public static DeploymentBuilder plainAdminClient(String namespaceName, String adminName, String bootstrapName, String additionalConfig) {
        return defaultAdminClient(namespaceName, adminName, bootstrapName, additionalConfig);
    }

    ///////////////////////////////////////////
    //          default admin client
    ///////////////////////////////////////////

    /**
     * Serves as base for all types pf admin clients (SCRAM_SHA, TLS, PLAINTEXT).
     */
    private static DeploymentBuilder defaultAdminClient(String namespaceName, String adminName, String bootstrapName, String additionalConfig) {
        Map<String, String> adminLabels = new HashMap<>();
        adminLabels.put(TestConstants.APP_POD_LABEL, TestConstants.ADMIN_CLIENT_NAME);
        adminLabels.put(TestConstants.KAFKA_ADMIN_CLIENT_LABEL_KEY, TestConstants.KAFKA_ADMIN_CLIENT_LABEL_VALUE);
        adminLabels.put(TestConstants.DEPLOYMENT_TYPE, DeploymentTypes.AdminClient.name());
        adminLabels.put(TestConstants.APP_CONTROLLER_LABEL, adminName);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        return new DeploymentBuilder()
            .withNewMetadata()
                .withNamespace(namespaceName)
                .withLabels(adminLabels)
                .withName(adminName)
            .endMetadata()
            .withNewSpec()
                .withNewSelector()
                    .addToMatchLabels(adminLabels)
                .endSelector()
                .withNewTemplate()
                    .withNewMetadata()
                        .withName(adminName)
                        .withNamespace(namespaceName)
                        .withLabels(adminLabels)
                    .endMetadata()
                    .withNewSpecLike(podSpecBuilder.build())
                        .addNewContainer()
                            .withName(adminName)
                            .withImagePullPolicy(TestConstants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                            .withImage(Environment.TEST_CLIENTS_IMAGE)
                            .addNewEnv()
                                .withName("BOOTSTRAP_SERVERS")
                                .withValue(bootstrapName)
                            .endEnv()
                            .addNewEnv()
                                .withName("ADDITIONAL_CONFIG")
                                .withValue(additionalConfig)
                            .endEnv()
                            .addNewEnv()
                                // use custom config folder for admin-client, so we don't need to use service account etc.
                                .withName("CONFIG_FOLDER_PATH")
                                .withValue("/tmp")
                            .endEnv()
                            .withCommand("sleep")
                            .withArgs("infinity")
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    ///////////////////////////////////////////
    //   Admin Client Configuration and envs
    ///////////////////////////////////////////

    /**
     * Constructs the (SASL) SCRAM configuration string from a secret for an admin client based on the user and security protocol.
     * Works for SASL-PLAIN and SASL-SSL alike.
     *
     * @param namespace the namespace in which the secret is stored
     * @param userName the name of the user (also used as the secret name) to fetch the SASL JAAS config
     * @param securityProtocol the security protocol to use (either SASL_PLAINTEXT or SASL_SSL)
     * @return a {@link String} containing the SASL mechanism, security protocol, and the SASL JAAS configuration
     */
    private static String getAdminClientScramConfig(String namespace, String userName, SecurityProtocol securityProtocol) {
        final String saslJaasConfigEncrypted = kubeClient().getSecret(namespace, userName).getData().get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = Util.decodeFromBase64(saslJaasConfigEncrypted);

        return "sasl.mechanism=SCRAM-SHA-512\n" +
            "security.protocol=" + securityProtocol + "\n" +
            "sasl.jaas.config=" + saslJaasConfigDecrypted;
    }

    /**
     * Creates an {@link EnvVar} environment variable for the Kafka cluster CA certificate, used for
     * configuring Kafka clients to trust the cluster's CA, thereby enabling TlS and ScramSha over TlS communication.
     *
     * @param clusterName the name of the Kafka cluster, used to derive the name of the Kubernetes secret containing the CA certificate
     * @return an {@link EnvVar} instance representing the CA certificate environment variable
     */
    private static EnvVar getClusterCaCertEnv(String clusterName) {
        return  new EnvVarBuilder()
            .withName("CA_CRT")
            .withNewValueFrom()
                .withNewSecretKeyRef()
                    .withName(KafkaResources.clusterCaCertificateSecretName(clusterName))
                    .withKey("ca.crt")
                .endSecretKeyRef()
            .endValueFrom()
            .build();
    }

    /**
     * Generates a list of {@link EnvVar} environment variables for TLS configuration, including the user's certificate and key.
     * These are extracted from Kubernetes secrets associated with the specified user. This setup is necessary for TLS (only) client
     * authentication against a Kafka cluster.
     *
     * @param userName the name of the user, which corresponds to the Kubernetes secret names containing the user's TLS certificate and key
     * @return a {@link List} of {@link EnvVar} instances for configuring a Kafka client with TLS
     */
    private static List<EnvVar> buildTLSUserCredentials(String userName) {
        EnvVar userCertificate = new EnvVarBuilder()
            .withName("USER_CRT")
            .withNewValueFrom()
                .withNewSecretKeyRef()
                    .withName(userName)
                    .withKey("user.crt")
                .endSecretKeyRef()
            .endValueFrom()
            .build();

        EnvVar userPrivateKey = new EnvVarBuilder()
            .withName("USER_KEY")
            .withNewValueFrom()
                .withNewSecretKeyRef()
                    .withName(userName)
                    .withKey("user.key")
                .endSecretKeyRef()
            .endValueFrom()
            .build();

        return List.of(userCertificate, userPrivateKey);
    }
}
