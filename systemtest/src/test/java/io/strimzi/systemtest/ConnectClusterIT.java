/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Event;
import io.strimzi.test.ClusterOperator;
import io.strimzi.test.CmData;
import io.strimzi.test.ConnectCluster;
import io.strimzi.test.JUnitGroup;
import io.strimzi.test.KafkaCluster;
import io.strimzi.test.Namespace;
import io.strimzi.test.OpenShiftOnly;
import io.strimzi.test.Resources;
import io.strimzi.test.StrimziRunner;
import io.strimzi.test.k8s.Oc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Failed;
import static io.strimzi.systemtest.k8s.Events.FailedSync;
import static io.strimzi.systemtest.k8s.Events.FailedValidation;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.k8s.Events.Unhealthy;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.systemtest.matchers.Matchers.hasNoneOfReasons;
import static io.strimzi.systemtest.matchers.Matchers.valueOfCmEquals;
import static io.strimzi.test.TestUtils.map;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@RunWith(StrimziRunner.class)
@Namespace(ConnectClusterIT.NAMESPACE)
@ClusterOperator
@KafkaCluster(name = ConnectClusterIT.KAFKA_CLUSTER_NAME)
public class ConnectClusterIT extends AbstractClusterIT {

    private static final Logger LOGGER = LogManager.getLogger(ConnectClusterIT.class);

    public static final String NAMESPACE = "connect-cluster-test";
    public static final String KAFKA_CLUSTER_NAME = "connect-tests";
    public static final String CONNECT_CLUSTER_NAME = "my-cluster";
    public static final String KAFKA_CONNECT_BOOTSTRAP_SERVERS = KAFKA_CLUSTER_NAME + "-kafka:9092";
    public static final String KAFKA_CONNECT_BOOTSTRAP_SERVERS_ESCAPED = KAFKA_CLUSTER_NAME + "-kafka\\:9092";
    public static final String CONNECT_CONFIG = "{\n" +
            "      \"bootstrap.servers\": \"" + KAFKA_CONNECT_BOOTSTRAP_SERVERS + "\"" +
            "    }";

    private static final String EXPECTED_CONFIG = "group.id=connect-cluster\\n" +
            "key.converter=org.apache.kafka.connect.json.JsonConverter\\n" +
            "internal.key.converter.schemas.enable=false\\n" +
            "value.converter=org.apache.kafka.connect.json.JsonConverter\\n" +
            "bootstrap.servers=" + KAFKA_CONNECT_BOOTSTRAP_SERVERS_ESCAPED + "\\n" +
            "config.storage.topic=connect-cluster-configs\\n" +
            "status.storage.topic=connect-cluster-status\\n" +
            "offset.storage.topic=connect-cluster-offsets\\n" +
            "internal.key.converter=org.apache.kafka.connect.json.JsonConverter\\n" +
            "internal.value.converter.schemas.enable=false\\n" +
            "internal.value.converter=org.apache.kafka.connect.json.JsonConverter\\n";

    @Test
    @JUnitGroup(name = "regression")
    @Resources(value = "../examples/templates/cluster-operator", asAdmin = true)
    @OpenShiftOnly
    public void testDeployConnectClusterViaTemplate() {
        Oc oc = (Oc) this.kubeClient;
        String clusterName = "openshift-my-connect-cluster";
        oc.newApp("strimzi-connect", map("CLUSTER_NAME", clusterName,
                "KAFKA_CONNECT_BOOTSTRAP_SERVERS", KAFKA_CONNECT_BOOTSTRAP_SERVERS));
        String deploymentName = clusterName + "-connect";
        oc.waitForDeployment(deploymentName);
        oc.deleteByName("cm", clusterName);
        oc.waitForResourceDeletion("deployment", deploymentName);
    }

    @Test
    @JUnitGroup(name = "acceptance")
    @ConnectCluster(name = "my-cluster", connectConfig = CONNECT_CONFIG)
    public void testDeployUndeploy() {
        LOGGER.info("Looks like the connect cluster my-cluster deployed OK");

        String podName = kubeClient.list("Pod").stream().filter(n -> n.startsWith("my-cluster-connect-")).findFirst().get();
        String kafkaPodJson = kubeClient.getResourceAsJson("pod", podName);

        assertEquals(EXPECTED_CONFIG.replaceAll("\\p{P}", ""), getValueFromJson(kafkaPodJson,
                globalVariableJsonPathBuilder("KAFKA_CONNECT_CONFIGURATION")));
    }

    @Test
    @JUnitGroup(name = "acceptance")
    @ConnectCluster(name = "jvm-resource", connectConfig = CONNECT_CONFIG,
        nodes = 1,
        config = {
                @CmData(key = "resources", value = "{ \"limits\": {\"memory\": \"400M\", \"cpu\": 2}, " +
                        "\"requests\": {\"memory\": \"300M\", \"cpu\": 1} }"),
                @CmData(key = "jvmOptions", value = "{\"-Xmx\": \"200m\", \"-Xms\": \"200m\"}")
        })
    public void testJvmAndResources() {
        String podName = kubeClient.list("Pod").stream().filter(n -> n.startsWith("jvm-resource-connect-")).findFirst().get();
        assertResources(NAMESPACE, podName,
                "400M", "2", "300M", "1");
        assertExpectedJavaOpts(podName,
                "-Xmx200m", "-Xms200m");
    }

    @Test
    @JUnitGroup(name = "regression")
    @ConnectCluster(name = CONNECT_CLUSTER_NAME, connectConfig = CONNECT_CONFIG)
    public void testKafkaConnectScaleUpScaleDown() {
        // kafka cluster Connect already deployed via annotation
        LOGGER.info("Running kafkaConnectScaleUP {}", CONNECT_CLUSTER_NAME);

        List<String> connectPods = kubeClient.listResourcesByLabel("pod", "strimzi.io/type=kafka-connect");
        int initialReplicas = connectPods.size();
        assertEquals(1, initialReplicas);
        final int scaleTo = initialReplicas + 1;

        LOGGER.info("Scaling up to {}", scaleTo);
        replaceCm(CONNECT_CLUSTER_NAME, "nodes", String.valueOf(initialReplicas + 1));
        kubeClient.waitForDeployment(kafkaConnectName(CONNECT_CLUSTER_NAME));
        connectPods = kubeClient.listResourcesByLabel("pod", "strimzi.io/type=kafka-connect");
        assertEquals(scaleTo, connectPods.size());
        for (String pod : connectPods) {
            kubeClient.waitForPod(pod);
            List<Event> events = getEvents("Pod", pod);
            assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
            assertThat(events, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));
        }

        LOGGER.info("Scaling down to {}", initialReplicas);
        replaceCm(CONNECT_CLUSTER_NAME, "nodes", String.valueOf(initialReplicas));
        while (kubeClient.listResourcesByLabel("pod", "strimzi.io/type=kafka-connect").size() == scaleTo) {
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

    @Test
    @JUnitGroup(name = "regression")
    @ConnectCluster(name = CONNECT_CLUSTER_NAME, connectConfig = CONNECT_CONFIG)
    public void testForUpdateValuesInConnectCM() {
        List<String> connectPods = kubeClient.listResourcesByLabel("pod", "strimzi.io/type=kafka-connect");

        String conncectConfig = "{\n" +
                "      \"bootstrap.servers\": \"" + KAFKA_CONNECT_BOOTSTRAP_SERVERS + "\",\n" +
                "      \"config.storage.replication.factor\": \"1\",\n" +
                "      \"offset.storage.replication.factor\": \"1\",\n" +
                "      \"status.storage.replication.factor\": \"1\"\n" +
                "    }";
        Map<String, String> changes = new HashMap<>();
        changes.put("connect-config", conncectConfig);
        changes.put("healthcheck-delay", "61");
        changes.put("healthcheck-timeout", "6");
        replaceCm(CONNECT_CLUSTER_NAME, changes);

        kubeClient.waitForDeployment(kafkaConnectName(CONNECT_CLUSTER_NAME));
        for (int i = 0; i < connectPods.size(); i++) {
            kubeClient.waitForResourceDeletion("pod", connectPods.get(i));
        }
        LOGGER.info("Verify values after update");
        String configMapAfter = kubeClient.get("cm", CONNECT_CLUSTER_NAME);
        assertThat(configMapAfter, valueOfCmEquals("healthcheck-delay", "61"));
        assertThat(configMapAfter, valueOfCmEquals("healthcheck-timeout", "6"));
        connectPods = kubeClient.listResourcesByLabel("pod", "strimzi.io/type=kafka-connect");
        for (int i = 0; i < connectPods.size(); i++) {
            String connectPodJson = kubeClient.getResourceAsJson("pod", connectPods.get(i));
            assertThat(connectPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(61)));
            assertThat(connectPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(6)));
            assertThat(connectPodJson, containsString("config.storage.replication.factor=1"));
            assertThat(connectPodJson, containsString("offset.storage.replication.factor=1"));
            assertThat(connectPodJson, containsString("status.storage.replication.factor=1"));
        }
    }
}