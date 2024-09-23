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
import io.strimzi.systemtest.templates.kubernetes.SecretTemplates;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.utils.specific.CruiseControlUtils.CRUISE_CONTROL_DEFAULT_PORT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
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
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3, 3)
            .editOrNewSpec()
                .withNewCruiseControl()
                    .withConfig(config)
                .endCruiseControl()
            .endSpec()
            .build());

        LOGGER.info("----> CRUISE CONTROL DEPLOYMENT STATE ENDPOINT <----");
        CruiseControlUtils.ApiResult response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.HttpMethod.GET,
                CruiseControlUtils.Scheme.HTTP, CRUISE_CONTROL_DEFAULT_PORT, CruiseControlEndpoints.STATE.toString(), "");
        String responseText = response.getResponseText();
        int responseCode = response.getResponseCode();

        LOGGER.info("Verifying that {} REST API is available using HTTP request without credentials", CRUISE_CONTROL_NAME);
        assertThat(responseCode, is(200));
        assertThat(responseText, containsString("RUNNING"));
        assertThat(responseText, containsString("NO_TASK_IN_PROGRESS"));
    }

    /**
     * @description This test case verifies the creation and usage of CruiseControl's API users.
     *
     * @steps
     *  1. - Create NodePools for the Kafka cluster
     *     - NodePools are created
     *  2. - Create Secret containing the `arnost: heslo, USER` in the `.key` field
     *     - Secret is correctly created
     *  3. - Deploy Kafka with CruiseControl containing configuration for the CC API users, with reference to the Secret (and its `.key` value) created in
     *       previous step
     *     - Kafka cluster with CruiseControl are deployed, the CC API users configuration is applied
     *  4. - Do request to CruiseControl's API, specifically to `/state` endpoint with `arnost:heslo` user
     *     - Request is successful and response contains information about state of the CruiseControl
     *
     * @usecase
     *  - cruise-control-api
     */
    @ParallelNamespaceTest
    void testCruiseControlAPIUsers() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String ccApiUserSecretName = "cc-api-users";
        final String ccApiUser = "arnost: heslo, USER\n";

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(
            SecretTemplates.secret(testStorage.getNamespaceName(), ccApiUserSecretName, "key", ccApiUser).build(),
            KafkaTemplates.kafkaWithCruiseControl(testStorage.getNamespaceName(), testStorage.getClusterName(), 3, 3)
                .editOrNewSpec()
                    .withNewCruiseControl()
                        .withNewHashLoginServiceApiUsers()
                            .withNewValueFrom()
                                .withNewSecretKeyRef("key", ccApiUserSecretName, false)
                            .endValueFrom()
                        .endHashLoginServiceApiUsers()
                    .endCruiseControl()
                .endSpec()
                .build()
        );

        CruiseControlUtils.ApiResult response = CruiseControlUtils.callApi(
            testStorage.getNamespaceName(),
            CruiseControlUtils.HttpMethod.GET,
            CruiseControlUtils.Scheme.HTTPS,
            CRUISE_CONTROL_DEFAULT_PORT,
            CruiseControlEndpoints.STATE.toString(),
            "",
            "arnost:heslo"
        );

        String responseText = response.getResponseText();
        int responseCode = response.getResponseCode();

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
