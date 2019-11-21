/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(ACCEPTANCE)
public class CrdST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CrdST.class);

//    message: 'spec.validation.openAPIV3Schema.type: Required value: must not be empty
//    at the root'
//    reason: Violations
//    status: "True"
//    type: NonStructuralSchema

    @Tag(ACCEPTANCE)
    @Test
    public void testNoNonstructualSchemas() {
        List<CustomResourceDefinition> crds = kubeClient().listCustomResourceDefinition();
        List<String> nonstructural = crds.stream().filter(crd -> {
            return crd.getSpec().getGroup().endsWith("strimzi.io")
                && crd.getStatus().getConditions().stream().anyMatch(condition -> {
                return "NonStructuralSchema".equals(condition.getType())
                        && "True".equals(condition.getStatus());
            });
        }).map(crd -> {
            return crd.getSpec().getNames().getPlural() + "." + crd.getSpec().getGroup();
        }).collect(Collectors.toList());
        assertTrue(nonstructural.isEmpty(), "Nonstructural schemas: " + nonstructural);
    }

}
