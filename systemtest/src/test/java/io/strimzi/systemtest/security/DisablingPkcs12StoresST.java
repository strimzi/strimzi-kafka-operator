/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestTags.REGRESSION;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite for verifying that users can disable PKCS12 stores in CA and User secrets."),
    labels = {
        @Label(value = TestDocsLabels.SECURITY)
    }
)
public class DisablingPkcs12StoresST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(DisablingPkcs12StoresST.class);

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test verifies that PKCS12 stores are not generated in CA and User secrets when it is disabled in the Cluster Operator configuration."),
        steps = {
            @Step(value = "Create a Kafka cluster.", expected = "Kafka cluster is deployed."),
            @Step(value = "Create a KafkaUser with TLS authentication.", expected = "New KafkaUser is created."),
            @Step(value = "Verify that Clients and Cluster CA secrets have no PKCS12 store and no password for it.", expected = "PKCS12 store and password are not present."),
            @Step(value = "Verify that Kafka User secret has no PKCS12 store and no password for it.", expected = "PKCS12 store and password are not present.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    void testDisabledPKCS12Stores() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        LOGGER.info("Deploying Kafka cluster with mixed nodes (3 replicas)");

        // Create dedicated controller and broker KafkaNodePools and Kafka CR
        int replicas = 3;
        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.mixedPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), testStorage.getClusterName(), replicas).build(),
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), replicas).build()
        );

        String username = "paulmcgrath";
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), username, testStorage.getClusterName()).build());

        // Check that there are no PKCS12 files
        Secret clusterCaCertSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.clusterCaCertificateSecretName(testStorage.getClusterName())).get();
        checkSecretForPkcs12Files(clusterCaCertSecret);

        Secret clientsCaCertSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(KafkaResources.clientsCaCertificateSecretName(testStorage.getClusterName())).get();
        checkSecretForPkcs12Files(clientsCaCertSecret);

        Secret userSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(testStorage.getNamespaceName()).withName(username).get();
        checkSecretForPkcs12Files(userSecret);
    }

    private void checkSecretForPkcs12Files(Secret secret) {
        secret.getData().keySet()
                .stream()
                .filter(key -> key.endsWith(".p12"))
                .findAny()
                .ifPresent(key -> {
                    throw new AssertionError("PKCS12 file " + key + " found in secret " + secret.getMetadata().getName());
                });

        secret.getData().keySet()
                .stream()
                .filter(key -> key.endsWith(".password"))
                .findAny()
                .ifPresent(key -> {
                    throw new AssertionError("PKCS12 password file " + key + " found in " + key + " secret " + secret.getMetadata().getName());
                });
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
                .getInstance()
                .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                        .withExtraEnvVars(new EnvVarBuilder()
                                .withName("STRIMZI_PKCS12_KEYSTORE_GENERATION")
                                .withValue("false")
                                .build())
                        .build())
                .install();
    }

}
