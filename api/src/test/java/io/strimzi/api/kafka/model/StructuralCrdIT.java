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

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionCondition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionVersion;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.annotations.VersionRange;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class StructuralCrdIT extends AbstractCrdIT {

    @Test
    public void kafkaV1Beta2IsStructuralWithCrdV1Beta1() {
        assumeKube1_16Plus();
        assertApiVersionsAreStructural("kafkas.kafka.strimzi.io",
                ApiVersion.V1BETA1,
                "040-Crd-kafka-crdApi-v1beta1.yaml",
                ApiVersion.parseRange("v1beta2+"));
    }

    @Test
    public void kafkaV1Beta2IsStructuralWithCrdV1() {
        assumeKube1_16Plus();
        assertApiVersionsAreStructural("kafkas.kafka.strimzi.io",
                ApiVersion.V1,
                "040-Crd-kafka-crdApi-v1.yaml",
                ApiVersion.parseRange("v1beta2+"));
    }

    private void assertApiVersionsAreStructural(String api, ApiVersion crdApiVersion, String crdYaml, VersionRange<ApiVersion> shouldBeStructural) {
        cluster.createCustomResources("src/test/resources/io/strimzi/api/kafka/model/" + crdYaml);
        try {
            waitForCrd("crd", "kafkas.kafka.strimzi.io");
            assertApiVersionsAreStructural(api, crdApiVersion, shouldBeStructural);
        } finally {
            cluster.deleteCustomResources("src/test/resources/io/strimzi/api/kafka/model/" + crdYaml);
        }
    }

    private void assertApiVersionsAreStructural(String api, ApiVersion crdApiVersion, VersionRange<ApiVersion> shouldBeStructural) {
        Pattern pattern = Pattern.compile("[^.]spec\\.versions\\[([0-9]+)\\]\\.[^,]*?");
        CustomResourceDefinition crd = cluster.client().getClient().apiextensions().v1().customResourceDefinitions().withName(api).get();
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
