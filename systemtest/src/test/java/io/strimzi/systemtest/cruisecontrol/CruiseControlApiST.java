/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlUserTaskStatus;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
public class CruiseControlApiST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlApiST.class);
    private static final String NAMESPACE = "cruise-control-api-test";
    private static final String CRUISE_CONTROL_NAME = "Cruise Control";
    private final String cruiseControlApiClusterName = "cruise-control-api-cluster-name";

    @Tag(ACCEPTANCE)
    @ParallelTest
    void testCruiseControlBasicAPIRequests()  {
        LOGGER.info("----> CRUISE CONTROL DEPLOYMENT STATE ENDPOINT <----");

        String response = CruiseControlUtils.callApi(CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.STATE);

        assertThat(response, is("Unrecognized endpoint in request '/state'\n" +
            "Supported POST endpoints: [ADD_BROKER, REMOVE_BROKER, FIX_OFFLINE_REPLICAS, REBALANCE, STOP_PROPOSAL_EXECUTION, PAUSE_SAMPLING, RESUME_SAMPLING, DEMOTE_BROKER, ADMIN, REVIEW, TOPIC_CONFIGURATION]\n"));

        response = CruiseControlUtils.callApi(CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.STATE);

        LOGGER.info("Verifying that {} REST API is available", CRUISE_CONTROL_NAME);

        assertThat(response, not(containsString("404")));
        assertThat(response, containsString("RUNNING"));
        assertThat(response, containsString("NO_TASK_IN_PROGRESS"));

        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent();

        LOGGER.info("----> KAFKA REBALANCE <----");

        response = CruiseControlUtils.callApi(CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.REBALANCE);

        assertThat(response, is("Unrecognized endpoint in request '/rebalance'\n" +
            "Supported GET endpoints: [BOOTSTRAP, TRAIN, LOAD, PARTITION_LOAD, PROPOSALS, STATE, KAFKA_CLUSTER_STATE, USER_TASKS, REVIEW_BOARD]\n"));

        LOGGER.info("Waiting for CC will have for enough metrics to be recorded to make a proposal ");
        CruiseControlUtils.waitForRebalanceEndpointIsReady();

        response = CruiseControlUtils.callApi(CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.REBALANCE);

        // all goals stats that contains
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

        assertThat(response, containsString("Cluster load after rebalance"));

        LOGGER.info("----> EXECUTION OF STOP PROPOSAL <----");

        response = CruiseControlUtils.callApi(CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.STOP);

        assertThat(response, is("Unrecognized endpoint in request '/stop_proposal_execution'\n" +
            "Supported GET endpoints: [BOOTSTRAP, TRAIN, LOAD, PARTITION_LOAD, PROPOSALS, STATE, KAFKA_CLUSTER_STATE, USER_TASKS, REVIEW_BOARD]\n"));

        response = CruiseControlUtils.callApi(CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.STOP);

        assertThat(response, containsString("Proposal execution stopped."));

        LOGGER.info("----> USER TASKS <----");

        response = CruiseControlUtils.callApi(CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.USER_TASKS);

        assertThat(response, is("Unrecognized endpoint in request '/user_tasks'\n" +
            "Supported POST endpoints: [ADD_BROKER, REMOVE_BROKER, FIX_OFFLINE_REPLICAS, REBALANCE, STOP_PROPOSAL_EXECUTION, PAUSE_SAMPLING, RESUME_SAMPLING, DEMOTE_BROKER, ADMIN, REVIEW, TOPIC_CONFIGURATION]\n"));

        response = CruiseControlUtils.callApi(CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.USER_TASKS);

        assertThat(response, containsString("GET"));
        assertThat(response, containsString(CruiseControlEndpoints.STATE.toString()));
        assertThat(response, containsString("POST"));
        assertThat(response, containsString(CruiseControlEndpoints.REBALANCE.toString()));
        assertThat(response, containsString(CruiseControlEndpoints.STOP.toString()));
        assertThat(response, containsString(CruiseControlUserTaskStatus.COMPLETED.toString()));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) throws Exception {
        installClusterOperator(extensionContext, NAMESPACE);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaWithCruiseControl(cruiseControlApiClusterName, 3, 3).build());
    }
}
