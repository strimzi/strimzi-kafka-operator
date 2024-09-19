/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
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

import static io.strimzi.systemtest.TestConstants.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.utils.specific.CruiseControlUtils.CRUISE_CONTROL_DEFAULT_PORT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@SuiteDoc(
    description = @Desc("This test case verifies that Cruise Control's basic API requests function correctly with security features disabled."),
    beforeTestSteps = {
        @Step(value = "Deploy the cluster operator", expected = "Cluster operator is deployed")
    },
    labels = {
        @Label(value = "cruise-control"),
    }
)
public class CruiseControlApiST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlApiST.class);
    private static final String CRUISE_CONTROL_NAME = "Cruise Control";

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test that verifies Cruise Control's basic API requests function correctly with security features disabled."),
        steps = {
            @Step(value = "Initialize the test storage.", expected = "Test storage object is created."),
            @Step(value = "Disable Cruise Control security and SSL in configuration.", expected = "Configuration map is set with security and SSL disabled."),
            @Step(value = "Create required Kafka and Cruise Control resources with disabled security.", expected = "Kafka and Cruise Control resources are deployed without enabling security."),
            @Step(value = "Call the Cruise Control state endpoint using HTTP without credentials.", expected = "Cruise Control state response is received with HTTP status code 200."),
            @Step(value = "Verify the Cruise Control state response.", expected = "Response indicates Cruise Control is RUNNING with NO_TASK_IN_PROGRESS.")
        },
        labels = {
            @Label(value = "cruise-control"),
        }
    )
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

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("This test case verifies the creation and usage of CruiseControl's API users."),
        steps = {
            @Step(value = "Initialize the test storage.", expected = "Test storage object is created."),
            @Step(value = "Create NodePools for the Kafka cluster", expected = "NodePools are created"),
            @Step(value = "Create Secret containing the `arnost: heslo, USER` in the `.key` field", expected = "Secret is correctly created"),
            @Step(value = "Deploy Kafka with CruiseControl containing configuration for the CC API users, with reference to the Secret (and its `.key` value) created in previous step", expected = "Kafka cluster with CruiseControl are deployed, the CC API users configuration is applied"),
            @Step(value = "Do request to CruiseControl's API, specifically to `/state` endpoint with `arnost:heslo` user", expected = "Request is successful and response contains information about state of the CruiseControl")
        },
        labels = {
            @Label(value = "cruise-control"),
        }
    )
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
