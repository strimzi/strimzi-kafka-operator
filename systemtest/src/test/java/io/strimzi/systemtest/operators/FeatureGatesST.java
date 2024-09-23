/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.strimzi.systemtest.Tags.REGRESSION;

/**
 * Feature Gates should give us additional options on
 * how to control and mature different behaviors in the operators.
 * https://github.com/strimzi/proposals/blob/main/022-feature-gates.md
 */
@Tag(REGRESSION)
@Disabled("Currently disabled as this class doesn't contain any tests. Once there is a new test, we should remove this")
public class FeatureGatesST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(FeatureGatesST.class);

    /**
     * Sets up a Cluster Operator with specified feature gates.
     *
     * @param extraFeatureGates A String representing additional feature gates (comma separated) to be
     *                          enabled or disabled for the Cluster Operator.
     */
    private void setupClusterOperatorWithFeatureGate(String extraFeatureGates) {
        List<EnvVar> coEnvVars = new ArrayList<>();
        coEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, extraFeatureGates, null));

        clusterOperator = this.clusterOperator.defaultInstallation()
            .withExtraEnvVars(coEnvVars)
            // necessary as each isolated test removes TEST_SUITE_NAMESPACE
            .withBindingsNamespaces(Arrays.asList(TestConstants.CO_NAMESPACE, Environment.TEST_SUITE_NAMESPACE))
            .createInstallation()
            .runInstallation();
    }
}
