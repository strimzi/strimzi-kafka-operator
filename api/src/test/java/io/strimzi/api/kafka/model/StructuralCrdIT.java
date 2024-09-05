/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionCondition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionVersion;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.annotations.VersionRange;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class StructuralCrdIT extends AbstractCrdIT {
    private final static Map<String, String> CRD_FILES = Map.of(
            "kafkas.kafka.strimzi.io", "040-Crd-kafka.yaml",
            "kafkaconnects.kafka.strimzi.io", "041-Crd-kafkaconnect.yaml",
            "kafkatopics.kafka.strimzi.io", "043-Crd-kafkatopic.yaml",
            "kafkausers.kafka.strimzi.io", "044-Crd-kafkauser.yaml",
            "kafkamirrormakers.kafka.strimzi.io", "045-Crd-kafkamirrormaker.yaml",
            "kafkabridges.kafka.strimzi.io", "046-Crd-kafkabridge.yaml",
            "kafkaconnectors.kafka.strimzi.io", "047-Crd-kafkaconnector.yaml",
            "kafkamirrormaker2s.kafka.strimzi.io", "048-Crd-kafkamirrormaker2.yaml",
            "kafkarebalances.kafka.strimzi.io", "049-Crd-kafkarebalance.yaml",
            "kafkanodepools.kafka.strimzi.io", "04A-Crd-kafkanodepool.yaml"
    );

    @BeforeEach
    public void beforeEach() {
        client = new KubernetesClientBuilder().build();
    }

    @AfterEach
    public void afterEach() {
        client.close();
    }

    @Test
    public void v1Beta2IsStructuralWithCrdV1() {
        for (Map.Entry<String, String> crd : CRD_FILES.entrySet()) {
            assertApiVersionsAreStructural(crd.getKey(), TestUtils.USER_PATH + "/../packaging/install/cluster-operator/" + crd.getValue(), ApiVersion.parseRange("v1beta2+"));
        }
    }

    private void assertApiVersionsAreStructural(String crdName, String crdPath, VersionRange<ApiVersion> shouldBeStructural) {
        try {
            TestUtils.createCrd(client, crdName, crdPath);
            assertApiVersionsAreStructuralInApiextensionsV1(crdName, shouldBeStructural);
        } finally {
            TestUtils.deleteCrd(client, crdName);
        }
    }

    private void assertApiVersionsAreStructuralInApiextensionsV1(String api, VersionRange<ApiVersion> shouldBeStructural) {
        Pattern pattern = Pattern.compile("[^.]spec\\.versions\\[([0-9]+)]\\.[^,]*?");

        CustomResourceDefinition crd = client.apiextensions().v1().customResourceDefinitions().withName(api).get();

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
