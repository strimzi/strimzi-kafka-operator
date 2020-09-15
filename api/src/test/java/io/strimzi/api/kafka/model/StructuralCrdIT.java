/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionCondition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionVersion;
import io.strimzi.crdgenerator.ApiVersion;
import io.strimzi.crdgenerator.VersionRange;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class StructuralCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "structuralcrd-it";

    @Test
    public void kafkaV1Beta2IsStructural() {
        assumeKube1_16Plus();
        assertApiVersionsAreStructural("kafkas.kafka.strimzi.io", ApiVersion.parseRange("v1beta2+"));
    }

    private void assertApiVersionsAreStructural(String api, VersionRange<ApiVersion> shouldBeStructural) {
        Pattern pattern = Pattern.compile("[^.]spec\\.versions\\[([0-9]+)\\]\\.[^,]*?");
        CustomResourceDefinition crd = cluster.client().getClient().customResourceDefinitions().withName(api).get();
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
            pattern.matcher(first.get().getMessage()).results().forEach(match -> {
                Integer index = Integer.valueOf(match.group(1));
                ApiVersion nonStructuralVersion = indexedVersions.get(index);
                if (shouldBeStructural.contains(nonStructuralVersion)) {
                    Assertions.fail(api + "/ " + nonStructuralVersion + " should be structural but there's a complaint about " + match.group());
                }
            });
        }
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_KAFKA);
        waitForCrd("crd", "kafkas.kafka.strimzi.io");
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA);
        cluster.deleteNamespaces();
    }
}
