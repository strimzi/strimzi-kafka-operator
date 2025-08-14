/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.featuregates;

import io.strimzi.operator.common.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FeatureGatesTest {
    @Test
    public void testIndividualFeatureGates() {
        for (FeatureGates.FeatureGate gate : FeatureGates.NONE.allFeatureGates()) {
            FeatureGates enabled = new FeatureGates("+" + gate.getName());
            FeatureGates disabled = new FeatureGates("-" + gate.getName());

            assertThat(enabled.allFeatureGates().stream().filter(g -> gate.getName().equals(g.getName())).findFirst().orElseThrow().isEnabled(), is(true));
            assertThat(disabled.allFeatureGates().stream().filter(g -> gate.getName().equals(g.getName())).findFirst().orElseThrow().isEnabled(), is(false));
        }
    }

    @Test
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

    @Test
    public void testFeatureGatesParsing() {
        assertThat(new FeatureGates("+ServerSideApplyPhase1").serverSideApplyPhase1Enabled(), is(true));
        assertThat(new FeatureGates("-ServerSideApplyPhase1").serverSideApplyPhase1Enabled(), is(false));
        assertThat(new FeatureGates("   -ServerSideApplyPhase1   ").serverSideApplyPhase1Enabled(), is(false));

        assertThat(new FeatureGates("+UseConnectBuildWithBuildah").useConnectBuildWithBuildahEnabled(), is(true));
        assertThat(new FeatureGates("-UseConnectBuildWithBuildah").useConnectBuildWithBuildahEnabled(), is(false));
        assertThat(new FeatureGates("   -UseConnectBuildWithBuildah   ").useConnectBuildWithBuildahEnabled(), is(false));

        assertThat(new FeatureGates("-ServerSideApplyPhase1,-UseConnectBuildWithBuildah").serverSideApplyPhase1Enabled(), is(false));
        assertThat(new FeatureGates("-ServerSideApplyPhase1,-UseConnectBuildWithBuildah").useConnectBuildWithBuildahEnabled(), is(false));
        assertThat(new FeatureGates("  +ServerSideApplyPhase1    ,    +UseConnectBuildWithBuildah").serverSideApplyPhase1Enabled(), is(true));
        assertThat(new FeatureGates("  +ServerSideApplyPhase1    ,    +UseConnectBuildWithBuildah").useConnectBuildWithBuildahEnabled(), is(true));
        assertThat(new FeatureGates("+UseConnectBuildWithBuildah,-ServerSideApplyPhase1").serverSideApplyPhase1Enabled(), is(false));
        assertThat(new FeatureGates("+UseConnectBuildWithBuildah,-ServerSideApplyPhase1").useConnectBuildWithBuildahEnabled(), is(true));
    }

    @Test
    public void testFeatureGatesEquals() {
        FeatureGates fg = new FeatureGates("+ServerSideApplyPhase1");
        assertThat(fg, is(fg));
        assertThat(fg, is(new FeatureGates("+ServerSideApplyPhase1")));
        assertThat(fg, is(not(new FeatureGates("-ServerSideApplyPhase1"))));
    }

    @Test
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

    @Test
    public void testDuplicateFeatureGateWithSameValue() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> new FeatureGates("+ServerSideApplyPhase1,+ServerSideApplyPhase1"));
        assertThat(e.getMessage(), containsString("Feature gate ServerSideApplyPhase1 is configured multiple times"));
    }

    @Test
    public void testDuplicateFeatureGateWithDifferentValue() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> new FeatureGates("+ServerSideApplyPhase1,-ServerSideApplyPhase1"));
        assertThat(e.getMessage(), containsString("Feature gate ServerSideApplyPhase1 is configured multiple times"));
    }

    @Test
    public void testMissingSign() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> new FeatureGates("ServerSideApplyPhase1"));
        assertThat(e.getMessage(), containsString("ServerSideApplyPhase1 is not a valid feature gate configuration"));
    }

    @Test
    public void testNonExistingGate() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> new FeatureGates("+RandomGate"));
        assertThat(e.getMessage(), containsString("Unknown feature gate RandomGate found in the configuration"));
    }

    @Test
    public void testEnvironmentVariable()   {
        assertThat(new FeatureGates("").toEnvironmentVariable(), is(""));
        assertThat(new FeatureGates("+ServerSideApplyPhase1").toEnvironmentVariable(), is("+ServerSideApplyPhase1"));
        assertThat(new FeatureGates("-ServerSideApplyPhase1").toEnvironmentVariable(), is(""));
    }
}
