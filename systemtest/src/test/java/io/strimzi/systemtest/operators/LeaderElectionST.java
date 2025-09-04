/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.coordination.v1.Lease;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.HelmInstallation;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.resources.operator.YamlInstallation;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.nio.file.Files;
import java.nio.file.Paths;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Suite for testing Leader Election feature <br>
 *
 * The feature allows users to run Cluster operator in more than one replica <br>
 *
 * There will be always one leader, other replicas will stay in "standby" mode <br>
 *
 * The whole procedure of deploying CO with Leader Election enabled and many more is described in
 *
 * <a href="https://strimzi.io/docs/operators/in-development/configuring.html#assembly-using-multiple-cluster-operator-replicas-str">the documentation</a>
 */
@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Suite for testing Leader Election feature which allows users to run Cluster Operator in more than one replica. There will always be one leader, other replicas will stay in standby mode."),
    beforeTestSteps = {
        @Step(value = "Verify deployment files contain all needed environment variables for leader election.", expected = "Deployment files contain required leader election environment variables.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
public class LeaderElectionST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(LeaderElectionST.class);

    private static final EnvVar LEADER_DISABLED_ENV = new EnvVarBuilder()
        .withName("STRIMZI_LEADER_ELECTION_ENABLED")
        .withValue("false")
        .build();

    private static final String LEADER_MESSAGE = "I'm the new leader";

    @IsolatedTest
    @TestDoc(
        description = @Desc("This test verifies that leader election works correctly when running Cluster Operator with multiple replicas. It tests leader failover by causing the current leader to crash and verifying that a new leader is elected."),
        steps = {
            @Step(value = "Deploy Cluster Operator with 2 replicas and leader election enabled.", expected = "Cluster Operator is deployed with 2 replicas and leader election is active."),
            @Step(value = "Identify the current leader pod from the lease.", expected = "Current leader pod is identified from the lease holder identity."),
            @Step(value = "Cause the leader pod to crash by changing its image to invalid one.", expected = "Leader pod enters CrashLoopBackOff state."),
            @Step(value = "Wait for a new leader to be elected.", expected = "A different pod becomes the new leader and lease is updated."),
            @Step(value = "Verify new leader election logs.", expected = "New leader pod logs contain leader election message.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testLeaderElection() {
        // create CO with 2 replicas, wait for Deployment readiness and leader election
        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withReplicas(2)
                .build()
            )
            .install();

        Lease oldLease = KubeResourceManager.get().kubeClient().getClient().leases().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).withName(SetupClusterOperator.getInstance().getOperatorDeploymentName()).get();
        String oldLeaderPodName = oldLease.getSpec().getHolderIdentity();

        LOGGER.info("Changing image of the leader pod: {} to not available image - to cause CrashLoopBackOff and change of leader to second Pod (failover)", oldLeaderPodName);

        KubeResourceManager.get().kubeClient().getClient().pods()
            .inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace())
            .withName(oldLeaderPodName)
            .edit(pod -> new PodBuilder(pod)
            .editOrNewSpec()
                .editContainer(0)
                    .withImage("wrong-image/name:latest")
                .endContainer()
            .endSpec()
            .build()
        );

        PodUtils.waitUntilPodIsInCrashLoopBackOff(SetupClusterOperator.getInstance().getOperatorNamespace(), oldLeaderPodName);

        Lease currentLease = KubeResourceManager.get().kubeClient().getClient().leases().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).withName(SetupClusterOperator.getInstance().getOperatorDeploymentName()).get();
        String currentLeaderPodName = currentLease.getSpec().getHolderIdentity();

        String logFromNewLeader = StUtils.getLogFromPodByTime(SetupClusterOperator.getInstance().getOperatorNamespace(), currentLeaderPodName, SetupClusterOperator.getInstance().getOperatorDeploymentName(), "300s");

        LOGGER.info("Checking if the new leader is elected");
        assertThat("Log doesn't contains mention about election of the new leader", logFromNewLeader.contains(LEADER_MESSAGE), is(true));
        assertThat("Old and current leaders are same", oldLeaderPodName, not(equalTo(currentLeaderPodName)));
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("This test verifies that when leader election is disabled, no lease is created and no leader election messages appear in the logs."),
        steps = {
            @Step(value = "Deploy Cluster Operator with leader election disabled.", expected = "Cluster Operator is deployed with STRIMZI_LEADER_ELECTION_ENABLED=false."),
            @Step(value = "Verify no lease exists for the Cluster Operator.", expected = "No lease resource is created in the operator namespace."),
            @Step(value = "Check Cluster Operator logs for absence of leader election messages.", expected = "Logs do not contain leader election messages.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testLeaderElectionDisabled() {
        // Currently there is no way how to disable LeaderElection when deploying CO via Helm (duplicated envs)
        assumeTrue(!Environment.isHelmInstall());

        // create CO with 1 replicas and with disabled leader election, wait for Deployment readiness
        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withExtraEnvVars(LEADER_DISABLED_ENV)
                .build()
            )
            .install();

        String coPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(SetupClusterOperator.getInstance().getOperatorNamespace(), SetupClusterOperator.getInstance().getOperatorDeploymentName()).get(0).getMetadata().getName();
        Lease notExistingLease = KubeResourceManager.get().kubeClient().getClient().leases().inNamespace(SetupClusterOperator.getInstance().getOperatorNamespace()).withName(SetupClusterOperator.getInstance().getOperatorDeploymentName()).get();
        String logFromCoPod = StUtils.getLogFromPodByTime(SetupClusterOperator.getInstance().getOperatorNamespace(), coPodName, SetupClusterOperator.getInstance().getOperatorDeploymentName(), "300s");

        // Assert that the Lease does not exist
        assertThat("Lease for CO exists", notExistingLease, is(nullValue()));
        assertThat("Log contains message about leader election", logFromCoPod.contains(LEADER_MESSAGE), is(false));
    }

    void checkDeploymentFiles() throws Exception {
        String pathToDepFile = "";

        if (Environment.isHelmInstall()) {
            pathToDepFile = HelmInstallation.HELM_CHART + "templates/060-Deployment-strimzi-cluster-operator.yaml";
        } else {
            pathToDepFile = YamlInstallation.PATH_TO_CO_CONFIG;
        }

        String clusterOperatorDep = Files.readString(Paths.get(pathToDepFile));

        assertThat("Cluster Operator's Deployment doesn't contain 'STRIMZI_LEADER_ELECTION_ENABLED' env variable", clusterOperatorDep.contains("STRIMZI_LEADER_ELECTION_ENABLED"), is(true));
        assertThat("Cluster Operator's Deployment doesn't contain 'STRIMZI_LEADER_ELECTION_LEASE_NAME' env variable", clusterOperatorDep.contains("STRIMZI_LEADER_ELECTION_LEASE_NAME"), is(true));
        assertThat("Cluster Operator's Deployment doesn't contain 'STRIMZI_LEADER_ELECTION_LEASE_NAMESPACE' env variable", clusterOperatorDep.contains("STRIMZI_LEADER_ELECTION_LEASE_NAMESPACE"), is(true));
        assertThat("Cluster Operator's Deployment doesn't contain 'STRIMZI_LEADER_ELECTION_IDENTITY' env variable", clusterOperatorDep.contains("STRIMZI_LEADER_ELECTION_IDENTITY"), is(true));
    }

    @BeforeAll
    void setup() throws Exception {
        // skipping if install type is OLM
        // OLM installation doesn't support configuring number of replicas inside the subscription
        assumeTrue(!Environment.isOlmInstall());

        LOGGER.info("Checking if Deployment files for install type: {} contains all needed env variables for leader election", Environment.CLUSTER_OPERATOR_INSTALL_TYPE);
        checkDeploymentFiles();
    }
}
