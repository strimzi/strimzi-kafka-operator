/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.featuregates;

import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class FeatureGatesTest {
    @ParallelTest
    public void testIndividualFeatureGates() {
        for (FeatureGates.FeatureGate gate : FeatureGates.NONE.allFeatureGates()) {
            FeatureGates enabled = new FeatureGates("+" + gate.getName());
            FeatureGates disabled = new FeatureGates("-" + gate.getName());

            assertThat(enabled.allFeatureGates().stream().filter(g -> gate.getName().equals(g.getName())).findFirst().orElseThrow().isEnabled(), is(true));
            assertThat(disabled.allFeatureGates().stream().filter(g -> gate.getName().equals(g.getName())).findFirst().orElseThrow().isEnabled(), is(false));
        }
    }

    @ParallelTest
    public void testAllFeatureGates() {
        List<String> allEnabled = new ArrayList<>();
        List<String> allDisabled = new ArrayList<>();

        for (FeatureGates.FeatureGate gate : FeatureGates.NONE.allFeatureGates()) {
            allEnabled.add("+" + gate.getName());
            allDisabled.add("-" + gate.getName());
        }

        FeatureGates enabled = new FeatureGates(String.join(",", allEnabled));
        for (FeatureGates.FeatureGate gate : enabled.allFeatureGates()) {
            assertThat(gate.isEnabled(), is(true));
        }

        FeatureGates disabled = new FeatureGates(String.join(",", allDisabled));
        for (FeatureGates.FeatureGate gate : disabled.allFeatureGates()) {
            assertThat(gate.isEnabled(), is(false));
        }
    }

    @ParallelTest
    public void testFeatureGatesParsing() {
        // TODO: Once we have new FeatureGate, we should implement the checks as shown below
        // assertThat(new FeatureGates("+ContinueReconciliationOnManualRollingUpdateFailure").continueOnManualRUFailureEnabled(), is(true));
        // assertThat(new FeatureGates("").continueOnManualRUFailureEnabled(), is(false));
        // assertThat(new FeatureGates("      ").continueOnManualRUFailureEnabled(), is(false));
        // TODO: Add more tests with various feature gate combinations once we have multiple feature gates again.
        //       The commented out code below shows the tests we used to have with multiple feature gates.
        //assertThat(new FeatureGates("-UseKRaft,").useKRaftEnabled(), is(false));
        //assertThat(new FeatureGates("-UseKRaft,").continueOnManualRUFailureEnabled(), is(false));
        //assertThat(new FeatureGates("  +UseKRaft    ,    +ContinueReconciliationOnManualRollingUpdateFailure").useKRaftEnabled(), is(true));
        //assertThat(new FeatureGates("  +UseKRaft    ,    +ContinueReconciliationOnManualRollingUpdateFailure").continueOnManualRUFailureEnabled(), is(true));
        //assertThat(new FeatureGates("+ContinueReconciliationOnManualRollingUpdateFailure,-UseKRaft").useKRaftEnabled(), is(false));
        //assertThat(new FeatureGates("+ContinueReconciliationOnManualRollingUpdateFailure,-UseKRaft").continueOnManualRUFailureEnabled(), is(true));
    }

    @ParallelTest
    public void testFeatureGatesEquals() {
        // TODO: Once we have new FeatureGate, we should implement the checks as shown below
        // FeatureGates fg = new FeatureGates("+ContinueReconciliationOnManualRollingUpdateFailure");
        // assertThat(fg, is(fg));
        // assertThat(fg, is(new FeatureGates("+ContinueReconciliationOnManualRollingUpdateFailure")));
        // assertThat(fg, is(not(new FeatureGates(""))));
    }

    @ParallelTest
    public void testEmptyFeatureGates() {
        List<FeatureGates> emptyFeatureGates = List.of(
                new FeatureGates(null),
                new FeatureGates(""),
                new FeatureGates("  "),
                new FeatureGates("    "),
                FeatureGates.NONE);

        for (FeatureGates fgs : emptyFeatureGates)  {
            for (FeatureGates.FeatureGate fg : fgs.allFeatureGates()) {
                assertThat(fg.isEnabled(), is(fg.isEnabledByDefault()));
            }
        }
    }

    @ParallelTest
    public void testDuplicateFeatureGateWithSameValue() {
        // TODO: Once we have new FeatureGate, we should implement the checks as shown below
        // InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> new FeatureGates("+ContinueReconciliationOnManualRollingUpdateFailure,+ContinueReconciliationOnManualRollingUpdateFailure"));
        // assertThat(e.getMessage(), containsString("Feature gate ContinueReconciliationOnManualRollingUpdateFailure is configured multiple times"));
    }

    @ParallelTest
    public void testDuplicateFeatureGateWithDifferentValue() {
        // TODO: Once we have new FeatureGate, we should implement the checks as shown below
        // InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> new FeatureGates("+ContinueReconciliationOnManualRollingUpdateFailure,"));
        // assertThat(e.getMessage(), containsString("Feature gate ContinueReconciliationOnManualRollingUpdateFailure is configured multiple times"));
    }

    @ParallelTest
    public void testMissingSign() {
        // TODO: Once we have new FeatureGate, we should implement the checks as shown below
        // InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> new FeatureGates("ContinueReconciliationOnManualRollingUpdateFailure"));
        // assertThat(e.getMessage(), containsString("ContinueReconciliationOnManualRollingUpdateFailure is not a valid feature gate configuration"));
    }

    @ParallelTest
    public void testNonExistingGate() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> new FeatureGates("+RandomGate"));
        assertThat(e.getMessage(), containsString("Unknown feature gate RandomGate found in the configuration"));
    }

    @ParallelTest
    public void testEnvironmentVariable()   {
        assertThat(new FeatureGates("").toEnvironmentVariable(), is(""));
        // TODO: Once we have new FeatureGate, we should implement the checks as shown below
        // assertThat(new FeatureGates("-ContinueReconciliationOnManualRollingUpdateFailure").toEnvironmentVariable(), is("-ContinueReconciliationOnManualRollingUpdateFailure"));
        // assertThat(new FeatureGates("+ContinueReconciliationOnManualRollingUpdateFailure").toEnvironmentVariable(), is(""));
    }
}
