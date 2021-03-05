/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinitionVersion;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.oneOf;

public class CliTestUtils {
    public static final String USER_PATH = System.getProperty("user.dir");
    public static final String CRD_V1_TOPIC = USER_PATH + "/../api/src/test/resources/io/strimzi/api/kafka/model/043-Crd-kafkatopic.yaml";
    public static final String CRD_V1_KAFKA = USER_PATH + "/../api/src/test/resources/io/strimzi/api/kafka/model/040-Crd-kafka.yaml";
    public static final String CRD_V1_KAFKA_CONNECT = USER_PATH + "/../api/src/test/resources/io/strimzi/api/kafka/model/041-Crd-kafkaconnect.yaml";
    public static final String CRD_V1_KAFKA_CONNECT_S2I = USER_PATH + "/../api/src/test/resources/io/strimzi/api/kafka/model/042-Crd-kafkaconnects2i.yaml";
    public static final String CRD_V1_KAFKA_USER = USER_PATH + "/../api/src/test/resources/io/strimzi/api/kafka/model/044-Crd-kafkauser.yaml";
    public static final String CRD_V1_KAFKA_MIRROR_MAKER = USER_PATH + "/../api/src/test/resources/io/strimzi/api/kafka/model/045-Crd-kafkamirrormaker.yaml";
    public static final String CRD_V1_KAFKA_BRIDGE = USER_PATH + "/../api/src/test/resources/io/strimzi/api/kafka/model/046-Crd-kafkabridge.yaml";
    public static final String CRD_V1_KAFKA_MIRROR_MAKER_2 = USER_PATH + "/../api/src/test/resources/io/strimzi/api/kafka/model/048-Crd-kafkamirrormaker2.yaml";
    public static final String CRD_V1_KAFKA_CONNECTOR = USER_PATH + "/../api/src/test/resources/io/strimzi/api/kafka/model//047-Crd-kafkaconnector.yaml";
    public static final String CRD_V1_KAFKA_REBALANCE = USER_PATH + "/../api/src/test/resources/io/strimzi/api/kafka/model/049-Crd-kafkarebalance.yaml";

    /**
     * Creates all CRDs of extensions/v1beta1 version containing all Strimzi API versions
     *
     * @param cluster   Kubernetes cluster
     */
    public static void setupAllCrds(KubeClusterResource cluster)  {
        cluster.createCustomResources(TestUtils.CRD_KAFKA);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_CONNECT);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_CONNECT_S2I);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_MIRROR_MAKER);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_MIRROR_MAKER_2);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_BRIDGE);
        cluster.createCustomResources(TestUtils.CRD_TOPIC);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_USER);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_CONNECTOR);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_REBALANCE);

        waitForCrd(cluster, "kafkas.kafka.strimzi.io");
        waitForCrd(cluster, "kafkaconnects2is.kafka.strimzi.io");
        waitForCrd(cluster, "kafkaconnects.kafka.strimzi.io");
        waitForCrd(cluster, "kafkamirrormaker2s.kafka.strimzi.io");
        waitForCrd(cluster, "kafkamirrormakers.kafka.strimzi.io");
        waitForCrd(cluster, "kafkabridges.kafka.strimzi.io");
        waitForCrd(cluster, "kafkatopics.kafka.strimzi.io");
        waitForCrd(cluster, "kafkausers.kafka.strimzi.io");
        waitForCrd(cluster, "kafkaconnectors.kafka.strimzi.io");
        waitForCrd(cluster, "kafkarebalances.kafka.strimzi.io");
    }

    /**
     * Waits for CRDs to be deployed and recognized by the Kube cluster
     *
     * @param cluster   Kubernetes cluster
     * @param name      Name of the CRD
     */
    private static void waitForCrd(KubeClusterResource cluster, String name) {
        cluster.cmdClient().waitFor("crd", name, crd -> {
            JsonNode json = (JsonNode) crd;
            if (json != null
                    && json.hasNonNull("status")
                    && json.get("status").hasNonNull("conditions")) {
                return true;
            }

            return false;
        });
    }

    /**
     * Deletes all Strimzi CRDs
     *
     * @param cluster   Kubernetes cluster
     */
    public static void deleteAllCrds(KubeClusterResource cluster) {
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA);
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA_CONNECT);
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA_CONNECT_S2I);
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA_MIRROR_MAKER);
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA_MIRROR_MAKER_2);
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA_BRIDGE);
        cluster.deleteCustomResources(TestUtils.CRD_TOPIC);
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA_USER);
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA_CONNECTOR);
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA_REBALANCE);
    }

    /**
     * Checks the status of the CRDs after the upgrade is complete => v1beta2 should be the only stored version
     *
     * @param client    Kubernetes client
     */
    public static void crdStatusHasUpdatedStorageVersions(KubernetesClient client)    {
        for (String kind : AbstractCommand.STRIMZI_KINDS)  {
            String crdName = CrdUpgradeCommand.CRD_NAMES.get(kind);
            CustomResourceDefinition crd = client.apiextensions().v1beta1().customResourceDefinitions().withName(crdName).get();

            assertThat(crd.getStatus().getStoredVersions(), hasItem("v1beta2"));
            assertThat(crd.getStatus().getStoredVersions(), hasItem(not("v1beta1")));
            assertThat(crd.getStatus().getStoredVersions(), hasItem(not("v1alpha1")));
        }
    }

    /**
     * Checks the status of the CRDs in the middle of the upgrade after the spec has been changed but not the status
     * => v1beta2 and one of v1beta1 and v1alpha1 (depends if the resource has v1beta1 or not) should be stored
     *
     * @param client    Kubernetes client
     */
    public static void crdStatusHasNotUpdatedStorageVersions(KubernetesClient client)    {
        for (String kind : AbstractCommand.STRIMZI_KINDS)  {
            String crdName = CrdUpgradeCommand.CRD_NAMES.get(kind);
            CustomResourceDefinition crd = client.apiextensions().v1beta1().customResourceDefinitions().withName(crdName).get();

            assertThat(crd.getStatus().getStoredVersions(), hasItem("v1beta2"));
            assertThat(crd.getStatus().getStoredVersions(), hasItem(oneOf("v1alpha1", "v1beta1")));
        }
    }

    /**
     * Checks the spec of the CRDs after the upgrade. v1beta2 should be the stored version, all versions should be served.
     *
     * @param client    Kubernetes client
     */
    public static void crdSpecHasUpdatedStorage(KubernetesClient client)    {
        for (String kind : AbstractCommand.STRIMZI_KINDS)  {
            String crdName = CrdUpgradeCommand.CRD_NAMES.get(kind);
            CustomResourceDefinition crd = client.apiextensions().v1beta1().customResourceDefinitions().withName(crdName).get();

            List<String> allVersions = crd.getSpec().getVersions().stream().map(CustomResourceDefinitionVersion::getName).collect(toList());
            List<String> storedVersions = crd.getSpec().getVersions().stream().filter(CustomResourceDefinitionVersion::getStorage).map(CustomResourceDefinitionVersion::getName).collect(toList());
            List<String> servedVersions = crd.getSpec().getVersions().stream().filter(CustomResourceDefinitionVersion::getServed).map(CustomResourceDefinitionVersion::getName).collect(toList());

            assertThat(storedVersions, hasItem("v1beta2"));
            assertThat(storedVersions, hasItem(not("v1alpha1")));
            assertThat(storedVersions, hasItem(not("v1beta1")));
            assertThat(servedVersions, is(allVersions));
        }
    }

    /**
     * Checks that the CRDs have the expected initial state => v1beta2 is present and served but not stored.
     *
     * @param client    Kubernetes client
     */
    public static void crdHasTheExpectedInitialState(KubernetesClient client)    {
        for (String kind : AbstractCommand.STRIMZI_KINDS)  {
            String crdName = CrdUpgradeCommand.CRD_NAMES.get(kind);
            CustomResourceDefinition crd = client.apiextensions().v1beta1().customResourceDefinitions().withName(crdName).get();

            List<String> allVersions = crd.getSpec().getVersions().stream().map(CustomResourceDefinitionVersion::getName).collect(toList());
            List<String> storedVersions = crd.getSpec().getVersions().stream().filter(CustomResourceDefinitionVersion::getStorage).map(CustomResourceDefinitionVersion::getName).collect(toList());
            List<String> servedVersions = crd.getSpec().getVersions().stream().filter(CustomResourceDefinitionVersion::getServed).map(CustomResourceDefinitionVersion::getName).collect(toList());

            assertThat(storedVersions, hasItem(not("v1beta2")));
            assertThat(storedVersions, hasItem(oneOf("v1alpha1", "v1beta1")));
            assertThat(servedVersions, is(allVersions));

            assertThat(crd.getStatus().getStoredVersions(), hasItem(not("v1beta2")));
            assertThat(crd.getStatus().getStoredVersions(), hasItem(oneOf("v1alpha1", "v1beta1")));
        }
    }

    /**
     * Creates all CRDs of extensions/v1 containing only Strimzi v1beta2 API version
     *
     * @param cluster   Kubernetes cluster
     */
    public static void setupV1Crds(KubeClusterResource cluster)  {
        cluster.replaceCustomResources(CRD_V1_KAFKA);
        cluster.replaceCustomResources(CRD_V1_KAFKA_CONNECT);
        cluster.replaceCustomResources(CRD_V1_KAFKA_CONNECT_S2I);
        cluster.replaceCustomResources(CRD_V1_KAFKA_MIRROR_MAKER);
        cluster.replaceCustomResources(CRD_V1_KAFKA_MIRROR_MAKER_2);
        cluster.replaceCustomResources(CRD_V1_KAFKA_BRIDGE);
        cluster.replaceCustomResources(CRD_V1_TOPIC);
        cluster.replaceCustomResources(CRD_V1_KAFKA_USER);
        cluster.replaceCustomResources(CRD_V1_KAFKA_CONNECTOR);
        cluster.replaceCustomResources(CRD_V1_KAFKA_REBALANCE);

        waitForCrd(cluster, "kafkas.kafka.strimzi.io");
        waitForCrd(cluster, "kafkaconnects2is.kafka.strimzi.io");
        waitForCrd(cluster, "kafkaconnects.kafka.strimzi.io");
        waitForCrd(cluster, "kafkamirrormaker2s.kafka.strimzi.io");
        waitForCrd(cluster, "kafkamirrormakers.kafka.strimzi.io");
        waitForCrd(cluster, "kafkabridges.kafka.strimzi.io");
        waitForCrd(cluster, "kafkatopics.kafka.strimzi.io");
        waitForCrd(cluster, "kafkausers.kafka.strimzi.io");
        waitForCrd(cluster, "kafkaconnectors.kafka.strimzi.io");
        waitForCrd(cluster, "kafkarebalances.kafka.strimzi.io");
    }

    /**
     * Deletes all Strimzi CRDs (extensions/v1 version)
     *
     * @param cluster   Kubernetes cluster
     */
    public static void deleteV1Crds(KubeClusterResource cluster) {
        cluster.deleteCustomResources(CRD_V1_KAFKA);
        cluster.deleteCustomResources(CRD_V1_KAFKA_CONNECT);
        cluster.deleteCustomResources(CRD_V1_KAFKA_CONNECT_S2I);
        cluster.deleteCustomResources(CRD_V1_KAFKA_MIRROR_MAKER);
        cluster.deleteCustomResources(CRD_V1_KAFKA_MIRROR_MAKER_2);
        cluster.deleteCustomResources(CRD_V1_KAFKA_BRIDGE);
        cluster.deleteCustomResources(CRD_V1_TOPIC);
        cluster.deleteCustomResources(CRD_V1_KAFKA_USER);
        cluster.deleteCustomResources(CRD_V1_KAFKA_CONNECTOR);
        cluster.deleteCustomResources(CRD_V1_KAFKA_REBALANCE);
    }

    /**
     * Checks that the CRD now has only the v1beta2 version
     *
     * @param client    Kubernetes client
     */
    public static void crdHasV1Beta2Only(KubernetesClient client)    {
        for (String kind : AbstractCommand.STRIMZI_KINDS)  {
            String crdName = CrdUpgradeCommand.CRD_NAMES.get(kind);
            CustomResourceDefinition crd = client.apiextensions().v1beta1().customResourceDefinitions().withName(crdName).get();

            assertThat(crd.getSpec().getVersions().size(), is(1));
            assertThat(crd.getSpec().getVersions().stream().map(CustomResourceDefinitionVersion::getName).collect(toList()), contains("v1beta2"));
        }
    }
}
