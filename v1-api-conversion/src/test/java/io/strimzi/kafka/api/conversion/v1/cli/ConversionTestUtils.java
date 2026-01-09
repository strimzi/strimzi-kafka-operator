/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionVersion;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.Updatable;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.test.CrdUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.oneOf;

public class ConversionTestUtils {
    private static final JsonMapper JSON_MAPPER = new JsonMapper();
    public static final String USER_PATH = System.getProperty("user.dir");

    public static final String CRD_V1BETA2_TOPIC = USER_PATH + "/../api/src/test/resources/crds/v1beta2/043-Crd-kafkatopic.yaml";
    public static final String CRD_V1BETA2_KAFKA = USER_PATH + "/../api/src/test/resources/crds/v1beta2/040-Crd-kafka.yaml";
    public static final String CRD_V1BETA2_KAFKA_CONNECT = USER_PATH + "/../api/src/test/resources/crds/v1beta2/041-Crd-kafkaconnect.yaml";
    public static final String CRD_V1BETA2_KAFKA_USER = USER_PATH + "/../api/src/test/resources/crds/v1beta2/044-Crd-kafkauser.yaml";
    public static final String CRD_V1BETA2_KAFKA_BRIDGE = USER_PATH + "/../api/src/test/resources/crds/v1beta2/046-Crd-kafkabridge.yaml";
    public static final String CRD_V1BETA2_KAFKA_MIRROR_MAKER_2 = USER_PATH + "/../api/src/test/resources/crds/v1beta2/048-Crd-kafkamirrormaker2.yaml";
    public static final String CRD_V1BETA2_KAFKA_CONNECTOR = USER_PATH + "/../api/src/test/resources/crds/v1beta2/047-Crd-kafkaconnector.yaml";
    public static final String CRD_V1BETA2_KAFKA_REBALANCE = USER_PATH + "/../api/src/test/resources/crds/v1beta2/049-Crd-kafkarebalance.yaml";
    public static final String CRD_V1BETA2_KAFKA_NODE_POOL = USER_PATH + "/../api/src/test/resources/crds/v1beta2/045-Crd-kafkanodepool.yaml";
    public static final String CRD_V1BETA2_STRIMZI_POD_SET = USER_PATH + "/../api/src/test/resources/crds/v1beta2/042-Crd-strimzipodset.yaml";

    public static final String CRD_V1_TOPIC = USER_PATH + "/../api/src/test/resources/crds/v1/043-Crd-kafkatopic.yaml";
    public static final String CRD_V1_KAFKA = USER_PATH + "/../api/src/test/resources/crds/v1/040-Crd-kafka.yaml";
    public static final String CRD_V1_KAFKA_CONNECT = USER_PATH + "/../api/src/test/resources/crds/v1/041-Crd-kafkaconnect.yaml";
    public static final String CRD_V1_KAFKA_USER = USER_PATH + "/../api/src/test/resources/crds/v1/044-Crd-kafkauser.yaml";
    public static final String CRD_V1_KAFKA_BRIDGE = USER_PATH + "/../api/src/test/resources/crds/v1/046-Crd-kafkabridge.yaml";
    public static final String CRD_V1_KAFKA_MIRROR_MAKER_2 = USER_PATH + "/../api/src/test/resources/crds/v1/048-Crd-kafkamirrormaker2.yaml";
    public static final String CRD_V1_KAFKA_CONNECTOR = USER_PATH + "/../api/src/test/resources/crds/v1/047-Crd-kafkaconnector.yaml";
    public static final String CRD_V1_KAFKA_REBALANCE = USER_PATH + "/../api/src/test/resources/crds/v1/049-Crd-kafkarebalance.yaml";
    public static final String CRD_V1_KAFKA_NODE_POOL = USER_PATH + "/../api/src/test/resources/crds/v1/045-Crd-kafkanodepool.yaml";
    public static final String CRD_V1_STRIMZI_POD_SET = USER_PATH + "/../api/src/test/resources/crds/v1/042-Crd-strimzipodset.yaml";


    private ConversionTestUtils() {
    }
    /**
     * Creates all Strimzi v1beta2 CRDs in the Kubernetes cluster
     *
     * @param client    Kubernetes client
     */
    public static void createV1beta2Crds(KubernetesClient client)   {
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_TOPIC_NAME, CRD_V1BETA2_TOPIC);
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_USER_NAME, CRD_V1BETA2_KAFKA_USER);
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_NAME, CRD_V1BETA2_KAFKA);
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_BRIDGE_NAME, CRD_V1BETA2_KAFKA_BRIDGE);
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_CONNECT_NAME, CRD_V1BETA2_KAFKA_CONNECT);
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_CONNECTOR_NAME, CRD_V1BETA2_KAFKA_CONNECTOR);
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_NODE_POOL_NAME, CRD_V1BETA2_KAFKA_NODE_POOL);
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_MIRROR_MAKER_2_NAME, CRD_V1BETA2_KAFKA_MIRROR_MAKER_2);
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_REBALANCE_NAME, CRD_V1BETA2_KAFKA_REBALANCE);
        CrdUtils.createCrd(client, CrdUtils.CRD_STRIMZI_POD_SET_NAME, CRD_V1BETA2_STRIMZI_POD_SET);
    }

    /**
     * Creates all Strimzi CRDs in the Kubernetes cluster
     *
     * @param client    Kubernetes client
     */
    public static void createV1Crds(KubernetesClient client)   {
        createOrUpdateCrd(client, CrdUtils.CRD_KAFKA_TOPIC_NAME, CRD_V1_TOPIC);
        createOrUpdateCrd(client, CrdUtils.CRD_KAFKA_USER_NAME, CRD_V1_KAFKA_USER);
        createOrUpdateCrd(client, CrdUtils.CRD_KAFKA_NAME, CRD_V1_KAFKA);
        createOrUpdateCrd(client, CrdUtils.CRD_KAFKA_BRIDGE_NAME, CRD_V1_KAFKA_BRIDGE);
        createOrUpdateCrd(client, CrdUtils.CRD_KAFKA_CONNECT_NAME, CRD_V1_KAFKA_CONNECT);
        createOrUpdateCrd(client, CrdUtils.CRD_KAFKA_CONNECTOR_NAME, CRD_V1_KAFKA_CONNECTOR);
        createOrUpdateCrd(client, CrdUtils.CRD_KAFKA_NODE_POOL_NAME, CRD_V1_KAFKA_NODE_POOL);
        createOrUpdateCrd(client, CrdUtils.CRD_KAFKA_MIRROR_MAKER_2_NAME, CRD_V1_KAFKA_MIRROR_MAKER_2);
        createOrUpdateCrd(client, CrdUtils.CRD_KAFKA_REBALANCE_NAME, CRD_V1_KAFKA_REBALANCE);
        createOrUpdateCrd(client, CrdUtils.CRD_STRIMZI_POD_SET_NAME, CRD_V1_STRIMZI_POD_SET);
    }

    /**
     * Creates a CRD resource in the Kubernetes cluster
     *
     * @param client    Kubernetes client
     * @param crdName   Name of the CRD
     * @param crdPath   Path to the CRD YAML
     */
    public static void createOrUpdateCrd(KubernetesClient client, String crdName, String crdPath)   {
        client.apiextensions().v1()
                .customResourceDefinitions()
                .load(crdPath)
                .createOr(Updatable::update);
        client.apiextensions().v1()
                .customResourceDefinitions()
                .withName(crdName)
                .waitUntilCondition(CrdUtils::isCrdEstablished, 10, TimeUnit.SECONDS);
    }

    /**
     * Deletes all Strimzi CRDs in the Kubernetes cluster
     *
     * @param client    Kubernetes client
     */
    public static void deleteAllCrds(KubernetesClient client)   {
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_TOPIC_NAME);
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_USER_NAME);
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_NAME);
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_BRIDGE_NAME);
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_CONNECT_NAME);
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_CONNECTOR_NAME);
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_NODE_POOL_NAME);
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_MIRROR_MAKER_2_NAME);
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_REBALANCE_NAME);
        CrdUtils.deleteCrd(client, CrdUtils.CRD_STRIMZI_POD_SET_NAME);
    }

    private static MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> operation(KubernetesClient client, String kind, String group, ApiVersion apiVersion) {
        return client.genericKubernetesResources(group + "/" + apiVersion, kind);
    }

    public static MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> kafkaOperation(KubernetesClient client) {
        return operation(client, "Kafka", "kafka.strimzi.io", ApiVersion.V1BETA2);
    }

    public static MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> kafkaV1Operation(KubernetesClient client) {
        return operation(client, "Kafka", "kafka.strimzi.io", ApiVersion.V1);
    }

    public static MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> connectOperation(KubernetesClient client) {
        return operation(client, "KafkaConnect", "kafka.strimzi.io", ApiVersion.V1BETA2);
    }

    public static MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> connectV1Operation(KubernetesClient client) {
        return operation(client, "KafkaConnect", "kafka.strimzi.io", ApiVersion.V1);
    }

    public static MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> bridgeOperation(KubernetesClient client) {
        return operation(client, "KafkaBridge", "kafka.strimzi.io", ApiVersion.V1BETA2);
    }

    public static <T> T genericToTyped(GenericKubernetesResource genericResource, Class<T> type)    {
        JsonNode genericResourceJson = JSON_MAPPER.valueToTree(genericResource);
        return JSON_MAPPER.convertValue(genericResourceJson, type);
    }

    public static GenericKubernetesResource typedToGeneric(Object resource)    {
        JsonNode genericResourceJson = JSON_MAPPER.valueToTree(resource);
        return JSON_MAPPER.convertValue(genericResourceJson, GenericKubernetesResource.class);
    }

    /**
     * Checks the status of the CRDs after the upgrade is complete => v1beta2 should be the only stored version
     *
     * @param client    Kubernetes client
     */
    public static void crdStatusHasUpdatedStorageVersions(KubernetesClient client)    {
        for (String kind : AbstractCommand.STRIMZI_KINDS)  {
            String crdName = CrdUpgradeCommand.CRD_NAMES.get(kind);
            CustomResourceDefinition crd = client.apiextensions().v1().customResourceDefinitions().withName(crdName).get();

            assertThat(crd.getStatus().getStoredVersions(), hasItem("v1"));
            assertThat(crd.getStatus().getStoredVersions(), hasItem(not("v1beta2")));
            assertThat(crd.getStatus().getStoredVersions(), hasItem(not("v1beta1")));
            assertThat(crd.getStatus().getStoredVersions(), hasItem(not("v1alpha1")));
        }
    }

    /**
     * Checks the status of the CRDs in the middle of the upgrade after the spec has been changed but not the status
     * => v1beta2 and one of v1beta1 and v1alpha1 (depends on whether the resource has v1beta1 or not) should be stored
     *
     * @param client    Kubernetes client
     */
    public static void crdStatusHasNotUpdatedStorageVersions(KubernetesClient client)    {
        for (String kind : AbstractCommand.STRIMZI_KINDS)  {
            String crdName = CrdUpgradeCommand.CRD_NAMES.get(kind);
            CustomResourceDefinition crd = client.apiextensions().v1().customResourceDefinitions().withName(crdName).get();

            assertThat(crd.getStatus().getStoredVersions(), hasItem("v1"));
            assertThat(crd.getStatus().getStoredVersions(), hasItem(oneOf("v1alpha1", "v1beta1", "v1beta2")));
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
            CustomResourceDefinition crd = client.apiextensions().v1().customResourceDefinitions().withName(crdName).get();

            List<String> allVersions = crd.getSpec().getVersions().stream().map(CustomResourceDefinitionVersion::getName).collect(toList());
            List<String> storedVersions = crd.getSpec().getVersions().stream().filter(CustomResourceDefinitionVersion::getStorage).map(CustomResourceDefinitionVersion::getName).collect(toList());
            List<String> servedVersions = crd.getSpec().getVersions().stream().filter(CustomResourceDefinitionVersion::getServed).map(CustomResourceDefinitionVersion::getName).collect(toList());

            assertThat(storedVersions, hasItem("v1"));
            assertThat(storedVersions, hasItem(not("v1alpha1")));
            assertThat(storedVersions, hasItem(not("v1beta1")));
            assertThat(storedVersions, hasItem(not("v1beta2")));
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
            CustomResourceDefinition crd = client.apiextensions().v1().customResourceDefinitions().withName(crdName).get();

            List<String> allVersions = crd.getSpec().getVersions().stream().map(CustomResourceDefinitionVersion::getName).collect(toList());
            List<String> storedVersions = crd.getSpec().getVersions().stream().filter(CustomResourceDefinitionVersion::getStorage).map(CustomResourceDefinitionVersion::getName).collect(toList());
            List<String> servedVersions = crd.getSpec().getVersions().stream().filter(CustomResourceDefinitionVersion::getServed).map(CustomResourceDefinitionVersion::getName).collect(toList());

            assertThat(storedVersions, hasItem(not("v1")));
            assertThat(storedVersions, hasItem(oneOf("v1beta2")));
            assertThat(servedVersions, is(allVersions));

            assertThat(crd.getStatus().getStoredVersions(), hasItem(not("v1")));
            assertThat(crd.getStatus().getStoredVersions(), hasItem(oneOf("v1beta2")));
        }
    }

    /**
     * Checks that the CRD now has only the v1 version.
     *
     * @param client    Kubernetes client
     */
    public static void crdHasV1Only(KubernetesClient client)    {
        for (String kind : AbstractCommand.STRIMZI_KINDS)  {
            String crdName = CrdUpgradeCommand.CRD_NAMES.get(kind);
            CustomResourceDefinition crd = client.apiextensions().v1().customResourceDefinitions().withName(crdName).get();
            assertThat(crd.getSpec().getVersions().size(), is(1));
            assertThat(crd.getSpec().getVersions().stream().map(CustomResourceDefinitionVersion::getName).collect(toList()), contains("v1"));
        }
    }
}
