/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlUserTaskStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftWithoutUTONotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
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

        String response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.STATE,  CruiseControlUtils.SupportedSchemes.HTTPS, true);

        assertThat(response, is("Unrecognized endpoint in request '/state'\n" +
            "Supported POST endpoints: [ADD_BROKER, REMOVE_BROKER, FIX_OFFLINE_REPLICAS, REBALANCE, STOP_PROPOSAL_EXECUTION, PAUSE_SAMPLING, " +
                "RESUME_SAMPLING, DEMOTE_BROKER, ADMIN, REVIEW, TOPIC_CONFIGURATION, RIGHTSIZE, REMOVE_DISKS]\n"));

        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.STATE,  CruiseControlUtils.SupportedSchemes.HTTPS, true);

        LOGGER.info("Verifying that {} REST API is available", CRUISE_CONTROL_NAME);

        assertThat(response, not(containsString("404")));
        assertThat(response, containsString("RUNNING"));
        assertThat(response, containsString("NO_TASK_IN_PROGRESS"));

        // https://github.com/strimzi/strimzi-kafka-operator/issues/8864
        if (!Environment.isUnidirectionalTopicOperatorEnabled()) {
            CruiseControlUtils.verifyThatCruiseControlTopicsArePresent(testStorage.getNamespaceName());
        }
        LOGGER.info("----> KAFKA REBALANCE <----");

        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.REBALANCE,  CruiseControlUtils.SupportedSchemes.HTTPS, true);

        assertThat(response, is("Unrecognized endpoint in request '/rebalance'\n" +
            "Supported GET endpoints: [BOOTSTRAP, TRAIN, LOAD, PARTITION_LOAD, PROPOSALS, STATE, KAFKA_CLUSTER_STATE, USER_TASKS, REVIEW_BOARD]\n"));

        LOGGER.info("Waiting for CC will have for enough metrics to be recorded to make a proposal ");
        CruiseControlUtils.waitForRebalanceEndpointIsReady(testStorage.getNamespaceName());

        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.REBALANCE,  CruiseControlUtils.SupportedSchemes.HTTPS, true);

        // all goals stats that contains
        assertCCGoalsInResponse(response);

        assertThat(response, containsString("Cluster load after rebalance"));

        LOGGER.info("----> EXECUTION OF STOP PROPOSAL <----");

        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.STOP, CruiseControlUtils.SupportedSchemes.HTTPS, true);

        assertThat(response, is("Unrecognized endpoint in request '/stop_proposal_execution'\n" +
            "Supported GET endpoints: [BOOTSTRAP, TRAIN, LOAD, PARTITION_LOAD, PROPOSALS, STATE, KAFKA_CLUSTER_STATE, USER_TASKS, REVIEW_BOARD]\n"));

        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.STOP, CruiseControlUtils.SupportedSchemes.HTTPS, true);

        assertThat(response, containsString("Proposal execution stopped."));

        LOGGER.info("----> USER TASKS <----");

        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.USER_TASKS, CruiseControlUtils.SupportedSchemes.HTTPS, true);

        assertThat(response, is("Unrecognized endpoint in request '/user_tasks'\n" +
            "Supported POST endpoints: [ADD_BROKER, REMOVE_BROKER, FIX_OFFLINE_REPLICAS, REBALANCE, STOP_PROPOSAL_EXECUTION, PAUSE_SAMPLING, " +
                "RESUME_SAMPLING, DEMOTE_BROKER, ADMIN, REVIEW, TOPIC_CONFIGURATION, RIGHTSIZE, REMOVE_DISKS]\n"));

        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.USER_TASKS,  CruiseControlUtils.SupportedSchemes.HTTPS, true);

        assertThat(response, containsString("GET"));
        assertThat(response, containsString(CruiseControlEndpoints.STATE.toString()));
        assertThat(response, containsString("POST"));
        assertThat(response, containsString(CruiseControlEndpoints.REBALANCE.toString()));
        assertThat(response, containsString(CruiseControlEndpoints.STOP.toString()));
        assertThat(response, containsString(CruiseControlUserTaskStatus.COMPLETED.toString()));


        LOGGER.info("Verifying that {} REST API doesn't allow HTTP requests", CRUISE_CONTROL_NAME);

        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.STATE,  CruiseControlUtils.SupportedSchemes.HTTP, false);
        assertThat(response, not(containsString("RUNNING")));
        assertThat(response, not(containsString("NO_TASK_IN_PROGRESS")));

        LOGGER.info("Verifying that {} REST API doesn't allow unauthenticated requests", CRUISE_CONTROL_NAME);

        response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.STATE,  CruiseControlUtils.SupportedSchemes.HTTPS, false);
        assertThat(response, containsString("401 Unauthorized"));
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

        String response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.STATE, CruiseControlUtils.SupportedSchemes.HTTP, false);

        LOGGER.info("Verifying that {} REST API is available using HTTP request without credentials", CRUISE_CONTROL_NAME);

        assertThat(response, not(containsString("404")));
        assertThat(response, containsString("RUNNING"));
        assertThat(response, containsString("NO_TASK_IN_PROGRESS"));
    }

    @ParallelNamespaceTest
    void testCruiseControlAPIForScalingBrokersUpAndDown(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 5, 3).build());

        LOGGER.info("Checking if we are able to execute GET request on {} and {} endpoints", CruiseControlEndpoints.ADD_BROKER, CruiseControlEndpoints.REMOVE_BROKER);

        String response = CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.ADD_BROKER,  CruiseControlUtils.SupportedSchemes.HTTPS, true);

        assertThat(response, is("Unrecognized endpoint in request '/add_broker'\n" +
            "Supported GET endpoints: [BOOTSTRAP, TRAIN, LOAD, PARTITION_LOAD, PROPOSALS, STATE, KAFKA_CLUSTER_STATE, USER_TASKS, REVIEW_BOARD]\n"));

        response =  CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlEndpoints.REMOVE_BROKER,  CruiseControlUtils.SupportedSchemes.HTTPS, true);

        assertThat(response, is("Unrecognized endpoint in request '/remove_broker'\n" +
            "Supported GET endpoints: [BOOTSTRAP, TRAIN, LOAD, PARTITION_LOAD, PROPOSALS, STATE, KAFKA_CLUSTER_STATE, USER_TASKS, REVIEW_BOARD]\n"));

        LOGGER.info("Waiting for CC will have for enough metrics to be recorded to make a proposal ");
        CruiseControlUtils.waitForRebalanceEndpointIsReady(testStorage.getNamespaceName());

        response =  CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.ADD_BROKER,  CruiseControlUtils.SupportedSchemes.HTTPS, true, "?brokerid=3,4");

        assertCCGoalsInResponse(response);
        assertThat(response, containsString("Cluster load after adding broker [3, 4]"));

        response =  CruiseControlUtils.callApi(testStorage.getNamespaceName(), CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlEndpoints.REMOVE_BROKER,  CruiseControlUtils.SupportedSchemes.HTTPS, true, "?brokerid=3,4");

        assertCCGoalsInResponse(response);
        assertThat(response, containsString("Cluster load after removing broker [3, 4]"));
    }

    @ParallelNamespaceTest
    void testKafkaRebalanceAutoApprovalMechanism(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3).build());

        // KafkaRebalance with auto-approval
        resourceManager.createResourceWithWait(extensionContext, KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName())
            .editMetadata()
                .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL, "true")
            .endMetadata()
            .build());

        KafkaRebalanceUtils.doRebalancingProcessWithAutoApproval(new Reconciliation("test", KafkaRebalance.RESOURCE_KIND,
            testStorage.getNamespaceName(), testStorage.getClusterName()), testStorage.getNamespaceName(), testStorage.getClusterName());
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
