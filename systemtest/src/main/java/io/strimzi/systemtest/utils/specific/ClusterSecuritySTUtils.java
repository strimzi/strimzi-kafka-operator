/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityEncryptionType;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.storage.TestStorage;
import org.hamcrest.CoreMatchers;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 *  Provides auxiliary methods for working with Cluster Security configurations.
 */
public class ClusterSecuritySTUtils {
    /**
     * Annotation used on the Kafka custom resource to configure the security of the internal cluster communication
     */
    public static final String INTERNAL_CLUSTER_SECURITY_ANNOTATION = "strimzi.io/internal-cluster-security";

    private ClusterSecuritySTUtils() { }

    /**
     * Generates the Cluster Security annotation for the encryption and authentication types configured for the whole
     * test run through the {@link Environment} class.
     *
     * @return  Cluster Security annotation as a JSON string
     */
    public static String clusterSecurityAnnotation() {
        return clusterSecurityAnnotation(Environment.CLUSTER_SECURITY_ENCRYPTION, Environment.CLUSTER_SECURITY_AUTHENTICATION);
    }

    /**
     * Generates the Cluster Security annotation for the given encryption and authentication types
     *
     * @param encryption        Encryption type
     * @param authentication    Authentication type
     *
     * @return  Cluster Security annotation as a JSON string
     */
    public static String clusterSecurityAnnotation(ClusterSecurityEncryptionType encryption, ClusterSecurityAuthenticationType authentication) {
        return "{\"encryption\":{\"type\":\"" + encryption.toValue() + "\"},\"authentication\":{\"type\":\"" + authentication.toValue() + "\"}}";
    }

    /**
     * Checks that the cluster security status corresponds to the desired encryption and authentication types.
     *
     * @param testStorage       Test storage containing cluster information
     * @param encryption        Expected encryption type
     * @param authentication    Expected authentication type
     */
    public static void assertClusterSecurityStatus(TestStorage testStorage, ClusterSecurityEncryptionType encryption, ClusterSecurityAuthenticationType authentication) {
        assertThat(CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName())
                        .get().getStatus().getClusterSecurity(),
                CoreMatchers.is(Map.of(
                        "encryption", Map.of("type", encryption.toValue()),
                        "authentication", Map.of("type", authentication.toValue())
                )));
    }
}
