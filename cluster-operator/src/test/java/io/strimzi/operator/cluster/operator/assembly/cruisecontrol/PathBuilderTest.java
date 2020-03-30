/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly.cruisecontrol;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class PathBuilderTest {

    private static final String DEFAULT_QUERY = "?" +
            CruiseControlParameters.JSON.key + "=true" + "&" +
            CruiseControlParameters.DRY_RUN.key + "=true" + "&" +
            CruiseControlParameters.VERBOSE.key + "=false";

    private static final List<String> GOALS = Arrays.asList("goal.one", "goal.two", "goal.three", "goal.four", "goal.five");

    private String getExpectedRebalanceString() {

        StringBuilder expectedQuery = new StringBuilder(
                CruiseControlEndpoints.REBALANCE.path + "?" +
                        CruiseControlParameters.JSON.key + "=true&" +
                        CruiseControlParameters.DRY_RUN.key + "=false&" +
                        CruiseControlParameters.VERBOSE.key + "=true&" +
                        CruiseControlParameters.GOALS.key + "=");

        for (int i = 0; i < GOALS.size(); i++) {
            expectedQuery.append(GOALS.get(i));
            if (i < GOALS.size() - 1) {
                expectedQuery.append(",");
            }
        }

        return expectedQuery.toString();
    }
    @Test
    public void testQueryStringPair() {

        String path = new PathBuilder(CruiseControlEndpoints.STATE)
                .addParameter(CruiseControlParameters.JSON, "true")
                .addParameter(CruiseControlParameters.DRY_RUN, "true")
                .addParameter(CruiseControlParameters.VERBOSE, "false")
                .build();

        assertThat(path, containsString(DEFAULT_QUERY));

    }

    @Test
    public void testQueryStringList() {

        String path = new PathBuilder(CruiseControlEndpoints.REBALANCE)
                .addParameter(CruiseControlParameters.JSON, "true")
                .addParameter(CruiseControlParameters.DRY_RUN, "false")
                .addParameter(CruiseControlParameters.VERBOSE, "true")
                .addParameter(CruiseControlParameters.GOALS, GOALS)
                .build();


        assertThat(path, is(getExpectedRebalanceString()));

    }

    @Test
    public void testQueryRebalanceOptions() {

        RebalanceOptions options = new RebalanceOptions.RebalanceOptionsBuilder()
                .withVerboseResponse()
                .withFullRun()
                .withGoals(GOALS)
                .build();

        String path = new PathBuilder(CruiseControlEndpoints.REBALANCE)
                .addParameter(CruiseControlParameters.JSON, "true")
                .addRebalanceParameters(options).build();

        assertThat(path, is(getExpectedRebalanceString()));
    }

}
