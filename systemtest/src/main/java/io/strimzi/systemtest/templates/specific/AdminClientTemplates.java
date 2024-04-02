/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.specific;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
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

    public static Deployment scramShaAdminClient(String namespaceName, String userName, String adminName, String bootstrapName, String additionalConfig) {
        String finalAdditionalConfig = getAdminClientScramConfig(namespaceName, userName) + "\n" + additionalConfig;

        return defaultAdminClient(namespaceName, adminName, bootstrapName, finalAdditionalConfig).build();
    }

    public static DeploymentBuilder defaultAdminClient(String namespaceName, String adminName, String bootstrapName) {
        return defaultAdminClient(namespaceName, adminName, bootstrapName, "");
    }

    public static DeploymentBuilder defaultAdminClient(String namespaceName, String adminName, String bootstrapName, String additionalConfig) {
        Map<String, String> adminLabels = new HashMap<>();
        adminLabels.put(TestConstants.APP_POD_LABEL, TestConstants.ADMIN_CLIENT_NAME);
        adminLabels.put(TestConstants.KAFKA_ADMIN_CLIENT_LABEL_KEY, TestConstants.KAFKA_ADMIN_CLIENT_LABEL_VALUE);
        adminLabels.put(TestConstants.DEPLOYMENT_TYPE, DeploymentTypes.AdminClient.name());

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

    private static String getAdminClientScramConfig(String namespaceName, String userName) {
        final String saslJaasConfigEncrypted = kubeClient().getSecret(namespaceName, userName).getData().get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = Util.decodeFromBase64(saslJaasConfigEncrypted);

        return "sasl.mechanism=SCRAM-SHA-512\n" +
            "security.protocol=" + SecurityProtocol.SASL_PLAINTEXT + "\n" +
            "sasl.jaas.config=" + saslJaasConfigDecrypted;
    }
}
