/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinitionCondition;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinitionVersion;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.annotations.VersionRange;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class StructuralCrdIT extends AbstractCrdIT {
    Map<String, String> crdFiles = Map.of(
            "kafkas.kafka.strimzi.io", "040-Crd-kafka.yaml",
            "kafkaconnects.kafka.strimzi.io", "041-Crd-kafkaconnect.yaml",
            "kafkaconnects2is.kafka.strimzi.io", "042-Crd-kafkaconnects2i.yaml",
            "kafkatopics.kafka.strimzi.io", "043-Crd-kafkatopic.yaml",
            "kafkausers.kafka.strimzi.io", "044-Crd-kafkauser.yaml",
            "kafkamirrormakers.kafka.strimzi.io", "045-Crd-kafkamirrormaker.yaml",
            "kafkabridges.kafka.strimzi.io", "046-Crd-kafkabridge.yaml",
            "kafkaconnectors.kafka.strimzi.io", "047-Crd-kafkaconnector.yaml",
            "kafkamirrormaker2s.kafka.strimzi.io", "048-Crd-kafkamirrormaker2.yaml",
            "kafkarebalances.kafka.strimzi.io", "049-Crd-kafkarebalance.yaml"
    );
    
    @Test
    public void v1Beta2IsStructuralWithCrdV1Beta1() {
        assumeKube1_16Plus();

        for (Map.Entry<String, String> crd : crdFiles.entrySet()) {
            assertApiVersionsAreStructural(crd.getKey(),
                    ApiVersion.V1BETA1,
                    TestUtils.USER_PATH + "/./src/test/resources/io/strimzi/api/kafka/model/" + crd.getValue(),
                    ApiVersion.parseRange("v1beta2+"));
        }
    }

    @Test
    public void v1Beta2IsStructuralWithCrdV1() {
        assumeKube1_16Plus();

        for (Map.Entry<String, String> crd : crdFiles.entrySet()) {
            assertApiVersionsAreStructural(crd.getKey(),
                    ApiVersion.V1,
                    TestUtils.USER_PATH + "/../packaging/install/cluster-operator/" + crd.getValue(),
                    ApiVersion.parseRange("v1beta2+"));
        }
    }

    private void assertApiVersionsAreStructural(String api, ApiVersion crdApiVersion, String crdYaml, VersionRange<ApiVersion> shouldBeStructural) {
        cluster.createCustomResources(crdYaml);
        try {
            waitForCrd("crd", api);
            assertApiVersionsAreStructural(api, crdApiVersion, shouldBeStructural);
        } finally {
            cluster.deleteCustomResources(crdYaml);
        }
    }

    private void assertApiVersionsAreStructural(String api, ApiVersion crdApiVersion, VersionRange<ApiVersion> shouldBeStructural) {
        Pattern pattern = Pattern.compile("[^.]spec\\.versions\\[([0-9]+)\\]\\.[^,]*?");
        CustomResourceDefinition crd = cluster.client().getClient().customResourceDefinitions().withName(api).get();
        // We can't make the following assertion because the current version of fabric8 always requests
        // the CRD using v1beta1 api version, so the apiserver just replaces it and serves it.
        //assertEquals(crdApiVersion, ApiVersion.parse(crd.getApiVersion().replace("apiextensions.k8s.io/", "")));
        Set<ApiVersion> presentCrdApiVersions = crd.getSpec().getVersions().stream().map(v -> ApiVersion.parse(v.getName())).collect(Collectors.toSet());
        assertTrue(presentCrdApiVersions.contains(shouldBeStructural.lower()),
                "CRD has versions " + presentCrdApiVersions + " which doesn't include " + shouldBeStructural.lower() + " which should be structural");
        Map<Integer, ApiVersion> indexedVersions = new HashMap<>();
        int i = 0;
        for (CustomResourceDefinitionVersion version : crd.getSpec().getVersions()) {
            indexedVersions.put(i, ApiVersion.parse(version.getName()));
        }
        Optional<CustomResourceDefinitionCondition> first = crd.getStatus().getConditions().stream()
                .filter(cond ->
                        "NonStructuralSchema".equals(cond.getType())
                                && "True".equals(cond.getStatus()))
                .findFirst();
        if (first.isPresent()) {

            Matcher matcher = pattern.matcher(first.get().getMessage());
            while (matcher.find()) {
                Integer index = Integer.valueOf(matcher.group(1));
                ApiVersion nonStructuralVersion = indexedVersions.get(index);
                if (shouldBeStructural.contains(nonStructuralVersion)) {
                    fail(api + "/ " + nonStructuralVersion + " should be structural but there's a complaint about " + matcher.group());
                }
            }
        }
    }
}
