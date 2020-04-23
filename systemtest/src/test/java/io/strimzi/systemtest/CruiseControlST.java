/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

@Tag(REGRESSION)
public class CruiseControlST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlST.class);

    public static final String NAMESPACE = "cruise-control-test";

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();
    }

    @Test
    void testCCDeployment()  {
        KafkaResource.kafkaWithCruiseControl(CLUSTER_NAME, 3, 3).done();

        String ccStatusCommand = "curl -X GET localhost:9090/kafkacruisecontrol/state";
        String ccPodName = PodUtils.getFirstPodNameContaining("cruise-control");
        LOGGER.info("Using pod: " + ccPodName);
        String result = cmdKubeClient().execInPodContainer(ccPodName, "cruise-control",
                "/bin/bash", "-c", ccStatusCommand).out();

        assertThat(result, not(containsString("404")));
        assertThat(result, containsString("RUNNING"));
    }

}
