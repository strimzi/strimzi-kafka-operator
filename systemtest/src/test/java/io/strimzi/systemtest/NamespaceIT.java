/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.JUnitGroup;
import io.strimzi.test.Namespace;
import io.strimzi.test.OpenShiftOnly;
import io.strimzi.test.StrimziRunner;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.Oc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;


import static io.strimzi.systemtest.NamespaceIT.NAMESPACE1;
import static io.strimzi.systemtest.NamespaceIT.NAMESPACE2;
import static io.strimzi.test.TestUtils.map;
import static org.junit.Assert.assertEquals;

@RunWith(StrimziRunner.class)
@Namespace(NAMESPACE1)
@Namespace(NAMESPACE2)
public class NamespaceIT extends AbstractClusterIT {

    private static final Logger LOGGER = LogManager.getLogger(NamespaceIT.class);

    public static final String NAMESPACE1 = "kafka-cluster-test";
    public static final String NAMESPACE2 = "kafka-cluster-test2";

    @Test
    @JUnitGroup(name = "regression")
    @OpenShiftOnly
    public void testForWorkWithMultipleNamespaces() {
        String clusterName = "openshift-my-cluster";

        //Deploying cluster in first namespace
        KubeClient client1 = kubeClient.inNamespace(NAMESPACE1);
        client1.deployCOFromCommandLine(NAMESPACE1, "../examples/install/cluster-operator");
        client1.waitForDeployment("strimzi-cluster-operator");
        client1.installTemplates(NAMESPACE1, "../examples/templates/cluster-operator");
        Oc oc1 = (Oc) client1;
        oc1.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName));
        client1.waitForStatefulSet(zookeeperClusterName(clusterName), 3);
        client1.waitForStatefulSet(kafkaClusterName(clusterName), 3);

        //Deploying cluster in second namespace
        KubeClient client2 = kubeClient.inNamespace(NAMESPACE2);
        client2.deployCOFromCommandLine(NAMESPACE2, "../examples/install/cluster-operator");
        client2.waitForDeployment("strimzi-cluster-operator");
        client2.installTemplates(NAMESPACE2, "../examples/templates/cluster-operator");
        Oc oc2 = (Oc) client2;
        oc2.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName));
        client2.waitForStatefulSet(zookeeperClusterName(clusterName), 3);
        client2.waitForStatefulSet(kafkaClusterName(clusterName), 3);

        //Updating first namespace and verifying that second namespace is not affected
        replaceCm(clusterName, NAMESPACE1, "zookeeper-nodes", "1");
        client1.waitForResourceDeletion("pod", zookeeperPodName(clusterName, 2));
        client1.waitForResourceDeletion("pod", zookeeperPodName(clusterName, 1));
        assertEquals(1, client1.listResourcesByLabel("pod", "strimzi.io/name=openshift-my-cluster-zookeeper", NAMESPACE1).size());
        assertEquals(3, client2.listResourcesByLabel("pod", "strimzi.io/name=openshift-my-cluster-zookeeper", NAMESPACE2).size());

        //Updating second namespace and verifying that first namespace is not affected
        replaceCm(clusterName, NAMESPACE2, "kafka-nodes", "1");
        client2.waitForResourceDeletion("pod", kafkaPodName(clusterName, 2));
        client2.waitForResourceDeletion("pod", kafkaPodName(clusterName, 1));
        assertEquals(1, client2.listResourcesByLabel("pod", "strimzi.io/name=openshift-my-cluster-kafka", NAMESPACE2).size());
        assertEquals(3, client1.listResourcesByLabel("pod", "strimzi.io/name=openshift-my-cluster-kafka", NAMESPACE1).size());

        //Deleting cluster
        client1.deleteByName("cm", clusterName, NAMESPACE1);
        kubeClient.inNamespace(NAMESPACE1).waitForResourceDeletion("statefulset", kafkaClusterName(clusterName));
        kubeClient.inNamespace(NAMESPACE1).waitForResourceDeletion("statefulset", zookeeperClusterName(clusterName));
        client2.deleteByName("cm", clusterName, NAMESPACE2);
        client2.waitForResourceDeletion("statefulset", kafkaClusterName(clusterName));
        client2.waitForResourceDeletion("statefulset", zookeeperClusterName(clusterName));
    }

}
