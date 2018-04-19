/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Event;
import io.strimzi.test.ClusterController;
import io.strimzi.test.ConnectCluster;
import io.strimzi.test.KafkaCluster;
import io.strimzi.test.Namespace;
import io.strimzi.test.OpenShiftOnly;
import io.strimzi.test.Resources;
import io.strimzi.test.StrimziRunner;
import io.strimzi.test.k8s.Oc;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.strimzi.systemtest.k8s.Events.*;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.systemtest.matchers.Matchers.hasNoneOfReasons;
import static io.strimzi.test.TestUtils.map;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(StrimziRunner.class)
@Namespace(ConnectClusterTest.NAMESPACE)
@ClusterController
@KafkaCluster(name = ConnectClusterTest.KAFKA_CLUSTER_NAME)
public class ConnectClusterTest extends AbstractClusterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectClusterTest.class);

    private static final String KAFKA_CONNECT_NAME = "my-cluster-connect";

    public static final String NAMESPACE = "connect-cluster-test";
    public static final String KAFKA_CLUSTER_NAME = "connect-tests";
    public static final String BOOTSTRAP_SERVERS = KAFKA_CLUSTER_NAME + "-kafka:9092";

    @Test
    @Resources(value = "../examples/templates/cluster-controller", asAdmin = true)
    @OpenShiftOnly
    public void testDeployConnectClusterViaTemplate() {
        Oc oc = (Oc) this.kubeClient;
        String clusterName = "openshift-my-connect-cluster";
        oc.newApp("strimzi-connect", map("CLUSTER_NAME", clusterName,
                "KAFKA_CONNECT_BOOTSTRAP_SERVERS", BOOTSTRAP_SERVERS));
        String deploymentName = clusterName + "-connect";
        oc.waitForDeployment(deploymentName);
        oc.deleteByName("cm", clusterName);
        oc.waitForResourceDeletion("deployment", deploymentName);
    }

    @Test
    @ConnectCluster(name = "my-cluster", bootstrapServers = BOOTSTRAP_SERVERS)
    public void testDeployUndeploy() {
        LOGGER.info("Looks like the connect cluster my-cluster deployed OK");
    }

    @Test
    @ConnectCluster(name = "my-cluster", bootstrapServers = BOOTSTRAP_SERVERS)
    public void testKafkaConnectScaleUpScaleDown() {
        // kafka cluster Connect already deployed via annotation
        LOGGER.info("Running kafkaConnectScaleUP {}", KAFKA_CONNECT_NAME);

        List<String> connectPods = kubeClient.listResourcesByLabel("pod", "strimzi.io/type=kafka-connect");
        int initialReplicas= connectPods.size();
        assertEquals(1, initialReplicas);
        final int scaleTo = initialReplicas + 1;

        LOGGER.info("Scaling up to {}", scaleTo);
        replaceCm("my-cluster", "nodes", String.valueOf(initialReplicas + 1));
        kubeClient.waitForDeployment(KAFKA_CONNECT_NAME);
        connectPods = kubeClient.listResourcesByLabel("pod", "strimzi.io/type=kafka-connect");
        assertEquals(scaleTo, connectPods.size());
        for (String pod : connectPods) {
        List<Event> events = getEvents("Pod", pod);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        assertThat(events, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));
        }

        LOGGER.info("Scaling down to {}", initialReplicas);
        replaceCm("my-cluster", "nodes", String.valueOf(initialReplicas));
        while (kubeClient.listResourcesByLabel("pod", "strimzi.io/type=kafka-connect").size() == scaleTo){
            LOGGER.info("Waiting for connect pod deletion");
        }
        connectPods = kubeClient.listResourcesByLabel("pod", "strimzi.io/type=kafka-connect");
        assertEquals(initialReplicas, connectPods.size());
        for (String pod : connectPods) {
            List<Event> events = getEvents("Pod", pod);
            assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
            assertThat(events, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));
        }

    }


}
