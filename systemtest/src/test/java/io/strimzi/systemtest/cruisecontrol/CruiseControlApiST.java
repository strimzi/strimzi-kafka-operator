package io.strimzi.systemtest.cruisecontrol;

import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CruiseControlApiST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlConfigurationST.class);
    private static final String NAMESPACE = "cruise-control-api-test";

    private static final String CRUISE_CONTROL_NAME = "Cruise Control";

    @Order(1)
    @Tag(ACCEPTANCE)
    @Test
    void testCruiseControlDeploymentStateEndpoint()  {
        String response = CruiseControlUtils.call(CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlUtils.CruiseControlEndpoints.STATE);

        assertThat(response, is("Unrecognized endpoint in request '/STATE'\n" +
            "Supported POST endpoints: [ADD_BROKER, REMOVE_BROKER, FIX_OFFLINE_REPLICAS, REBALANCE, STOP_PROPOSAL_EXECUTION, PAUSE_SAMPLING, RESUME_SAMPLING, DEMOTE_BROKER, ADMIN, REVIEW, TOPIC_CONFIGURATION]\n"));

        response = CruiseControlUtils.call(CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlUtils.CruiseControlEndpoints.STATE);

        LOGGER.info("Verifying that {} REST API is available", CRUISE_CONTROL_NAME);

        assertThat(response, not(containsString("404")));
        assertThat(response, containsString("RUNNING"));
        assertThat(response, containsString("NO_TASK_IN_PROGRESS"));

        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent();
    }

    @Order(2)
    @Test
    void testRebalance() {
        String response = CruiseControlUtils.call(CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlUtils.CruiseControlEndpoints.REBALANCE);

        assertThat(response, is("Unrecognized endpoint in request '/REBALANCE'\n" +
            "Supported GET endpoints: [BOOTSTRAP, TRAIN, LOAD, PARTITION_LOAD, PROPOSALS, STATE, KAFKA_CLUSTER_STATE, USER_TASKS, REVIEW_BOARD]\n"));

        response = CruiseControlUtils.call(CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlUtils.CruiseControlEndpoints.REBALANCE);

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
    }

    @Order(3)
    @Test
    void testStopProposalExecution() {
        String response = CruiseControlUtils.call(CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlUtils.CruiseControlEndpoints.STOP_PROPOSAL_EXECUTION);

        assertThat(response, is("Unrecognized endpoint in request '/STOP_PROPOSAL_EXECUTION'\n" +
            "Supported GET endpoints: [BOOTSTRAP, TRAIN, LOAD, PARTITION_LOAD, PROPOSALS, STATE, KAFKA_CLUSTER_STATE, USER_TASKS, REVIEW_BOARD]\n"));

        response = CruiseControlUtils.call(CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlUtils.CruiseControlEndpoints.STOP_PROPOSAL_EXECUTION);

        assertThat(response, containsString("Proposal execution stopped."));
    }

    @Order(4)
    @Test
    void testUserTasks() {
        String response = CruiseControlUtils.call(CruiseControlUtils.SupportedHttpMethods.POST, CruiseControlUtils.CruiseControlEndpoints.USER_TASKS);

        assertThat(response, is("Unrecognized endpoint in request '/USER_TASKS'\n" +
            "Supported POST endpoints: [ADD_BROKER, REMOVE_BROKER, FIX_OFFLINE_REPLICAS, REBALANCE, STOP_PROPOSAL_EXECUTION, PAUSE_SAMPLING, RESUME_SAMPLING, DEMOTE_BROKER, ADMIN, REVIEW, TOPIC_CONFIGURATION]\n"));

        response = CruiseControlUtils.call(CruiseControlUtils.SupportedHttpMethods.GET, CruiseControlUtils.CruiseControlEndpoints.USER_TASKS);

        assertThat(response, containsString("GET"));
        assertThat(response, containsString("/kafkacruisecontrol/STATE"));
        assertThat(response, containsString("POST"));
        assertThat(response, containsString("/kafkacruisecontrol/REBALANCE"));
        assertThat(response, containsString("Completed"));
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();

        deployTestResources();
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) throws InterruptedException {
        super.recreateTestEnv(coNamespace, bindingsNamespaces);
        deployTestResources();
    }

    private void deployTestResources() {
        KafkaResource.kafkaWithCruiseControl(CLUSTER_NAME, 3, 3).done();
    }
}
