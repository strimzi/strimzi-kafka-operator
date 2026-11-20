/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class PathBuilderTest {

    private static final String DEFAULT_QUERY = "?" +
            CruiseControlParameters.JSON + "=true" + "&" +
            CruiseControlParameters.DRY_RUN + "=true" + "&" +
            CruiseControlParameters.VERBOSE + "=false";

    private static final List<String> GOALS = Arrays.asList("goal.one", "goal.two", "goal.three", "goal.four", "goal.five");

    private String getExpectedRebalanceString(boolean includeSkipHardGoalCheck) {
        StringBuilder expectedQuery = new StringBuilder(
                CruiseControlEndpoints.REBALANCE + "?" +
                        CruiseControlParameters.JSON + "=true&" +
                        CruiseControlParameters.DRY_RUN + "=false&" +
                        CruiseControlParameters.VERBOSE + "=true&" +
                        (includeSkipHardGoalCheck ? CruiseControlParameters.SKIP_HARD_GOAL_CHECK + "=false&" : "") +
                        CruiseControlParameters.EXCLUDED_TOPICS + "=test-.*&" +
                        CruiseControlParameters.GOALS + "=");

        StringBuilder goalStringBuilder = new StringBuilder();
        for (int i = 0; i < GOALS.size(); i++) {
            goalStringBuilder.append(GOALS.get(i));
            if (i < GOALS.size() - 1) {
                goalStringBuilder.append(",");
            }
        }

        expectedQuery.append(URLEncoder.encode(goalStringBuilder.toString(), StandardCharsets.UTF_8));
        return expectedQuery.toString();
    }
    @Test
    public void testQueryStringPair() {

        String path = new PathBuilder(CruiseControlEndpoints.STATE)
                .withParameter(CruiseControlParameters.DRY_RUN, "true")
                .withParameter(CruiseControlParameters.VERBOSE, "false")
                .withParameter(CruiseControlParameters.REBALANCE_DISK, "false")
                .build();

        assertThat(path, containsString(DEFAULT_QUERY));

    }

    @Test
    public void testQueryStringList() {

        String path = new PathBuilder(CruiseControlEndpoints.REBALANCE)
                .withParameter(CruiseControlParameters.DRY_RUN, "false")
                .withParameter(CruiseControlParameters.VERBOSE, "true")
                .withParameter(CruiseControlParameters.EXCLUDED_TOPICS, "test-.*")
                .withParameter(CruiseControlParameters.GOALS, String.join(",", GOALS))
                .build();


        assertThat(path, is(getExpectedRebalanceString(false)));

    }

    @Test
    public void testQueryRebalanceOptions() {
        RebalanceOptions options = new RebalanceOptions.RebalanceOptionsBuilder()
            .withConfigEntry(CruiseControlParameters.DRY_RUN.toString(), "false")
            .withConfigEntry(CruiseControlParameters.VERBOSE.toString(), "true")
            .withConfigEntry(CruiseControlParameters.EXCLUDED_TOPICS.toString(), "test-.*")
            .withConfigEntry(CruiseControlParameters.GOALS.toString(), String.join(",", GOALS))
            .build();

        String path = new PathBuilder(CruiseControlEndpoints.REBALANCE)
                .withParameters(options).build();

        assertThat(path, is(getExpectedRebalanceString(true)));
    }

}
