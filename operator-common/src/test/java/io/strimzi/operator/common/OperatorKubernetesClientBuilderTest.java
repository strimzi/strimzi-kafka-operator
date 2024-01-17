/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class OperatorKubernetesClientBuilderTest {
    
    @Test
    void testBuild() {
        OperatorKubernetesClientBuilder builder = new OperatorKubernetesClientBuilder("test-component", "1.0");
        KubernetesClient client = builder.build();
        assertNotNull(client, "A KubernetesClient should be returned");
        final String userAgent = client.getConfiguration().getUserAgent();
        assertEquals("test-component/1.0", userAgent, "The user agent should be set to the component name and version");
    }
}
