/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftWithoutUTONotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

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
    private final String cruiseControlApiClusterName = "cruise-control-api-cluster-name";

    @ParallelNamespaceTest
    @KRaftWithoutUTONotSupported()
    void testCruiseControlBasicAPIRequests(ExtensionContext extensionContext)  {
        final TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3).build());

        LOGGER.info("----> CRUISE CONTROL DEPLOYMENT STATE ENDPOINT <----");
        LOGGER.info("Verifying that {} REST API is available", CRUISE_CONTROL_NAME);
        CruiseControlUtils.ApiResult response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.HttpMethod.GET, CruiseControlEndpoints.STATE);
        String responseText = response.getResponseText();
        int responseCode = response.getResponseCode();

        assertThat(responseCode, is(200));
        assertThat(responseText, containsString("RUNNING"));
        assertThat(responseText, containsString("NO_TASK_IN_PROGRESS"));

        // https://github.com/strimzi/strimzi-kafka-operator/issues/8864
        if (!Environment.isUnidirectionalTopicOperatorEnabled()) {
            CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(testStorage.getNamespaceName());
        }

        LOGGER.info("----> KAFKA REBALANCE <----");
        LOGGER.info("Waiting for CC will have for enough metrics to be recorded to make a proposal ");
        CruiseControlUtils.waitForRebalanceEndpointIsReady(testStorage.getNamespaceName());
        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.HttpMethod.POST, CruiseControlEndpoints.REBALANCE);
        responseText = response.getResponseText();
        responseCode = response.getResponseCode();

        // all goals stats that contains
        assertCCGoalsInResponse(responseText);
        assertThat(responseCode, is(200));
        assertThat(responseText, containsString("Cluster load after rebalance"));

        LOGGER.info("----> EXECUTION OF STOP PROPOSAL <----");
        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.HttpMethod.POST, CruiseControlEndpoints.STOP);
        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.HttpMethod.POST, CruiseControlEndpoints.STOP);
        responseText = response.getResponseText();
        responseCode = response.getResponseCode();

        assertThat(responseCode, is(200));
        assertThat(responseText, containsString("Proposal execution stopped."));

        LOGGER.info("----> USER TASKS <----");
        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.HttpMethod.GET, CruiseControlEndpoints.USER_TASKS);
        responseText = response.getResponseText();
        responseCode = response.getResponseCode();

        assertThat(responseCode, is(200));
        assertThat(responseText, containsString("GET"));
        assertThat(responseText, containsString(CruiseControlEndpoints.STATE.toString()));
        assertThat(responseText, containsString("POST"));
        assertThat(responseText, containsString(CruiseControlEndpoints.REBALANCE.toString()));
        assertThat(responseText, containsString(CruiseControlEndpoints.STOP.toString()));
        assertThat(responseText, containsString(CruiseControlUserTaskStatus.COMPLETED.toString()));

        LOGGER.info("Verifying that {} REST API doesn't allow unauthorized requests", CRUISE_CONTROL_NAME);
        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.HttpMethod.GET, CruiseControlUtils.Scheme.HTTPS, CRUISE_CONTROL_DEFAULT_PORT, CruiseControlEndpoints.STATE, "", false);
        responseCode = response.getResponseCode();

        assertThat(responseCode, is(401));
    }

    @ParallelNamespaceTest
    void testCruiseControlBasicAPIRequestsWithSecurityDisabled(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        Map<String, Object> config = new HashMap<>();
        config.put("webserver.security.enable", "false");
        config.put("webserver.ssl.enable", "false");

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaWithCruiseControl(cruiseControlApiClusterName, 3, 3)
            .editOrNewSpec()
                .withNewCruiseControl()
                    .withConfig(config)
                .endCruiseControl()
            .endSpec()
            .build());

        LOGGER.info("----> CRUISE CONTROL DEPLOYMENT STATE ENDPOINT <----");
        CruiseControlUtils.ApiResult response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.HttpMethod.GET, CruiseControlUtils.Scheme.HTTP, CRUISE_CONTROL_DEFAULT_PORT, CruiseControlEndpoints.STATE, "", false);
        String responseText = response.getResponseText();
        int responseCode = response.getResponseCode();

        LOGGER.info("Verifying that {} REST API is available using HTTP request without credentials", CRUISE_CONTROL_NAME);
        assertThat(responseCode, is(200));
        assertThat(responseText, containsString("RUNNING"));
        assertThat(responseText, containsString("NO_TASK_IN_PROGRESS"));
    }

    @ParallelNamespaceTest
    void testCruiseControlAPIForScalingBrokersUpAndDown(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 5, 3).build());

        LOGGER.info("Waiting for CC will have for enough metrics to be recorded to make a proposal ");
        CruiseControlUtils.waitForRebalanceEndpointIsReady(testStorage.getNamespaceName());

        CruiseControlUtils.ApiResult response =  CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.HttpMethod.POST, CruiseControlUtils.Scheme.HTTPS, CRUISE_CONTROL_DEFAULT_PORT, CruiseControlEndpoints.ADD_BROKER, "?brokerid=3,4", true);
        String responseText = response.getResponseText();
        int responseCode = response.getResponseCode();

        assertThat(responseCode, is(200));
        assertCCGoalsInResponse(responseText);
        assertThat(responseText, containsString("Cluster load after adding broker [3, 4]"));

        response =  CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.HttpMethod.POST, CruiseControlUtils.Scheme.HTTPS, CRUISE_CONTROL_DEFAULT_PORT, CruiseControlEndpoints.REMOVE_BROKER, "?brokerid=3,4", true);
        responseText = response.getResponseText();
        responseCode = response.getResponseCode();

        assertThat(responseCode, is(200));
        assertCCGoalsInResponse(responseText);
        assertThat(responseText, containsString("Cluster load after removing broker [3, 4]"));
    }

    private void assertCCGoalsInResponse(String response) {
        assertThat(response, containsString("RackAwareGoal"));
        assertThat(response, containsString("ReplicaCapacityGoal"));
        assertThat(response, containsString("DiskCapacityGoal"));
        assertThat(response, containsString("NetworkInboundCapacityGoal"));
        assertThat(response, containsString("NetworkOutboundCapacityGoal"));
        assertThat(response, containsString("CpuCapacityGoal"));
        assertThat(response, containsString("ReplicaDistributionGoal"));
        assertThat(response, containsString("DiskUsageDistributionGoal"));
        assertThat(response, containsString("NetworkInboundUsageDistributionGoal"));
        assertThat(response, containsString("NetworkOutboundUsageDistributionGoal"));
        assertThat(response, containsString("CpuUsageDistributionGoal"));
        assertThat(response, containsString("TopicReplicaDistributionGoal"));
        assertThat(response, containsString("LeaderReplicaDistributionGoal"));
        assertThat(response, containsString("LeaderBytesInDistributionGoal"));
        assertThat(response, containsString("PreferredLeaderElectionGoal"));
    }

    @BeforeAll
    void setUp(final ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();
    }
}
