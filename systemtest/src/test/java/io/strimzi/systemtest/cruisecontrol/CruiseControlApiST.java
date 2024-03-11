/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.utils.specific.CruiseControlUtils.CRUISE_CONTROL_DEFAULT_PORT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@Tag(ACCEPTANCE)
public class CruiseControlApiST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlApiST.class);
    private static final String CRUISE_CONTROL_NAME = "Cruise Control";

    @ParallelNamespaceTest
    void testCruiseControlBasicAPIRequestsWithSecurityDisabled() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        Map<String, Object> config = new HashMap<>();
        config.put("webserver.security.enable", "false");
        config.put("webserver.ssl.enable", "false");

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3)
            .editOrNewSpec()
                .withNewCruiseControl()
                    .withConfig(config)
                .endCruiseControl()
            .endSpec()
            .build());

        LOGGER.info("----> CRUISE CONTROL DEPLOYMENT STATE ENDPOINT <----");
        CruiseControlUtils.ApiResult response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.HttpMethod.GET,
                CruiseControlUtils.Scheme.HTTP, CRUISE_CONTROL_DEFAULT_PORT, CruiseControlEndpoints.STATE.toString(), "", false);
        String responseText = response.getResponseText();
        int responseCode = response.getResponseCode();

        LOGGER.info("Verifying that {} REST API is available using HTTP request without credentials", CRUISE_CONTROL_NAME);
        assertThat(responseCode, is(200));
        assertThat(responseText, containsString("RUNNING"));
        assertThat(responseText, containsString("NO_TASK_IN_PROGRESS"));
    }

    @BeforeAll
    void setUp() {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();
    }
}
