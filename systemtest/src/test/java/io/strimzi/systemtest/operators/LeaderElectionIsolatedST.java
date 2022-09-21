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
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.annotations.IsolatedTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;
import java.util.List;

import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
@IsolatedSuite
public class LeaderElectionIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(LeaderElectionIsolatedST.class);

    /*
     These env variables are already inside packaging/../060-Deployment-strimzi-cluster-operator.yaml, but to be sure that
     we are setting everything correctly and the Leader Election is really enabled, we are adding it to additional envs
     */
    final EnvVar leaderEnabledEnv = new EnvVarBuilder()
        .withName("STRIMZI_LEADER_ELECTION_ENABLED")
        .withValue("true")
        .build();

    final EnvVar leaseNameEnv = new EnvVarBuilder()
        .withName("STRIMZI_LEADER_ELECTION_LEASE_NAME")
        .withValue(Constants.STRIMZI_DEPLOYMENT_NAME)
        .build();

    final EnvVar leaseNamespaceEnv = new EnvVarBuilder()
        .withName("STRIMZI_LEADER_ELECTION_LEASE_NAMESPACE")
        .withNewValueFrom()
            .withNewFieldRef()
                .withFieldPath("metadata.namespace")
            .endFieldRef()
        .endValueFrom()
        .build();

    final EnvVar leaderIdentityEnv = new EnvVarBuilder()
        .withName("STRIMZI_LEADER_ELECTION_IDENTITY")
        .withNewValueFrom()
            .withNewFieldRef()
                .withFieldPath("metadata.name")
            .endFieldRef()
        .endValueFrom()
        .build();

    final List<EnvVar> envList = Arrays.asList(leaderEnabledEnv, leaseNameEnv, leaseNamespaceEnv, leaderIdentityEnv);

    @IsolatedTest
    void testLeaderElection(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);

        clusterOperator = clusterOperator.defaultInstallation()
            .withExtensionContext(extensionContext)
            .withExtraEnvVars(envList)
            .withReplicas(2)
            .createInstallation()
            .runInstallation();

        Lease oldLease = kubeClient().getClient().leases().inNamespace(testStorage.getNamespaceName()).withName(Constants.STRIMZI_DEPLOYMENT_NAME).get();
        String oldLeaderPodName = oldLease.getSpec().getHolderIdentity();

        LOGGER.info("Changing image of the leader pod: {} to not available image - to cause CrashLoopBackOff and change of leader to second pod (failover)", oldLeaderPodName);

        kubeClient().editPod(testStorage.getNamespaceName(), oldLeaderPodName).edit(pod -> new PodBuilder(pod)
            .editOrNewSpec()
                .editContainer(0)
                    .withImage("wrong-image/name:latest")
                .endContainer()
            .endSpec()
            .build()
        );

        PodUtils.waitUntilPodIsInCrashLoopBackOff(testStorage.getNamespaceName(), oldLeaderPodName);

        Lease currentLease = kubeClient().getClient().leases().inNamespace(testStorage.getNamespaceName()).withName(Constants.STRIMZI_DEPLOYMENT_NAME).get();
        String currentLeaderPodName = currentLease.getSpec().getHolderIdentity();

        String logFromNewLeader = StUtils.getLogFromPodByTime(INFRA_NAMESPACE, currentLeaderPodName, Constants.STRIMZI_DEPLOYMENT_NAME, "300s");

        assertTrue(logFromNewLeader.contains("I'm the new leader"));
        assertNotEquals(oldLeaderPodName, currentLeaderPodName);
    }

    @BeforeAll
    void setup() {
        assumeTrue(!Environment.isOlmInstall());
        clusterOperator.unInstall();
    }
}
