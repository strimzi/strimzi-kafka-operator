/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.coordination.v1.Lease;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.resources.operator.specific.HelmResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.annotations.IsolatedTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
public class LeaderElectionST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(LeaderElectionST.class);

    private static final EnvVar LEADER_DISABLED_ENV = new EnvVarBuilder()
        .withName("STRIMZI_LEADER_ELECTION_ENABLED")
        .withValue("false")
        .build();

    private static final String LEADER_MESSAGE = "I'm the new leader";

    @IsolatedTest
    @Tag(ACCEPTANCE)
    void testLeaderElection() {
        // create CO with 2 replicas, wait for Deployment readiness and leader election
        clusterOperator = clusterOperator.defaultInstallation()
            .withExtensionContext(ResourceManager.getTestContext())
            .withReplicas(2)
            .createInstallation()
            .runInstallation();

        Lease oldLease = kubeClient().getClient().leases().inNamespace(clusterOperator.getDeploymentNamespace()).withName(clusterOperator.getClusterOperatorName()).get();
        String oldLeaderPodName = oldLease.getSpec().getHolderIdentity();

        LOGGER.info("Changing image of the leader pod: {} to not available image - to cause CrashLoopBackOff and change of leader to second Pod (failover)", oldLeaderPodName);

        kubeClient().editPod(clusterOperator.getDeploymentNamespace(), oldLeaderPodName).edit(pod -> new PodBuilder(pod)
            .editOrNewSpec()
                .editContainer(0)
                    .withImage("wrong-image/name:latest")
                .endContainer()
            .endSpec()
            .build()
        );

        PodUtils.waitUntilPodIsInCrashLoopBackOff(clusterOperator.getDeploymentNamespace(), oldLeaderPodName);

        Lease currentLease = kubeClient().getClient().leases().inNamespace(clusterOperator.getDeploymentNamespace()).withName(clusterOperator.getClusterOperatorName()).get();
        String currentLeaderPodName = currentLease.getSpec().getHolderIdentity();

        String logFromNewLeader = StUtils.getLogFromPodByTime(clusterOperator.getDeploymentNamespace(), currentLeaderPodName, clusterOperator.getClusterOperatorName(), "300s");

        LOGGER.info("Checking if the new leader is elected");
        assertThat("Log doesn't contains mention about election of the new leader", logFromNewLeader, containsString(LEADER_MESSAGE));
        assertThat("Old and current leaders are same", oldLeaderPodName, not(equalTo(currentLeaderPodName)));
    }

    @IsolatedTest
    void testLeaderElectionDisabled() {
        // Currently there is no way how to disable LeaderElection when deploying CO via Helm (duplicated envs)
        assumeTrue(!Environment.isHelmInstall());

        // create CO with 1 replicas and with disabled leader election, wait for Deployment readiness
        clusterOperator = clusterOperator.defaultInstallation()
            .withExtensionContext(ResourceManager.getTestContext())
            .withExtraEnvVars(Collections.singletonList(LEADER_DISABLED_ENV))
            .createInstallation()
            .runInstallation();

        String coPodName = kubeClient().listPodsByPrefixInName(clusterOperator.getDeploymentNamespace(), clusterOperator.getClusterOperatorName()).get(0).getMetadata().getName();
        Lease notExistingLease = kubeClient().getClient().leases().inNamespace(clusterOperator.getDeploymentNamespace()).withName(clusterOperator.getClusterOperatorName()).get();
        String logFromCoPod = StUtils.getLogFromPodByTime(clusterOperator.getDeploymentNamespace(), coPodName, clusterOperator.getClusterOperatorName(), "300s");

        // Assert that the Lease does not exist
        assertThat("Lease for CO exists", notExistingLease, is(nullValue()));

        assertThat("Log contains message about leader election", logFromCoPod, not(containsString(LEADER_MESSAGE)));
    }

    void checkDeploymentFiles() throws Exception {
        String pathToDepFile = "";

        if (Environment.isHelmInstall()) {
            pathToDepFile = HelmResource.HELM_CHART + "templates/060-Deployment-strimzi-cluster-operator.yaml";
        } else {
            pathToDepFile = BundleResource.PATH_TO_CO_CONFIG;
        }

        String clusterOperatorDep = Files.readString(Paths.get(pathToDepFile));

        assertThat(clusterOperatorDep, containsString("STRIMZI_LEADER_ELECTION_ENABLED"));
        assertThat(clusterOperatorDep, containsString("STRIMZI_LEADER_ELECTION_LEASE_NAME"));
        assertThat(clusterOperatorDep, containsString("STRIMZI_LEADER_ELECTION_LEASE_NAMESPACE"));
        assertThat(clusterOperatorDep, containsString("STRIMZI_LEADER_ELECTION_IDENTITY"));
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
