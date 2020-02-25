/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.OlmResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class OlmST extends BaseST {

    public static final String NAMESPACE = "olm-namespace";

    private static final Logger LOGGER = LogManager.getLogger(OlmST.class);

    @Test
    void testOlm() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1).done();
        LOGGER.info("Test");
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        cluster.setNamespace(cluster.getDefaultOlmvNamespace());
        OlmResource.clusterOperator(Constants.STRIMZI_DEPLOYMENT_NAME, cluster.getDefaultOlmvNamespace());
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) throws InterruptedException {
    }

    @Override
    protected void tearDownEnvironmentAfterEach() throws Exception {
    }
}
