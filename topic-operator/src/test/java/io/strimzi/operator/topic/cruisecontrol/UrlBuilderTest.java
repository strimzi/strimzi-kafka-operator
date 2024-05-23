/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.cruisecontrol;

import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class UrlBuilderTest {
    private static final List<String> GOALS = Arrays.asList("goal.one", "goal.two", "goal.three", "goal.four", "goal.five");
    
    @Test
    public void testUriWithSingleValueQueryParameters() {
        String expectedUrl = "https://localhost:9090" +
            CruiseControlEndpoints.STATE + "?" +
            CruiseControlParameters.JSON + "=true&" +
            CruiseControlParameters.DRY_RUN + "=false&" +
            CruiseControlParameters.VERBOSE + "=false";
        
        URI uri = new UrlBuilder("localhost", 9090, CruiseControlEndpoints.STATE, true)
            .withParameter(CruiseControlParameters.JSON, "true")
            .withParameter(CruiseControlParameters.DRY_RUN, "false")
            .withParameter(CruiseControlParameters.VERBOSE, "false")
            .build();
        assertThat(uri.toString(), is(expectedUrl));
    }

    @Test
    public void testUriWithListQueryParameters() {
        String expectedUrl = "https://localhost:9090" +
            CruiseControlEndpoints.REBALANCE + "?" +
                CruiseControlParameters.JSON + "=true&" +
                CruiseControlParameters.DRY_RUN + "=false&" +
                CruiseControlParameters.VERBOSE + "=true&" +
                CruiseControlParameters.SKIP_HARD_GOAL_CHECK + "=false&" +
                CruiseControlParameters.EXCLUDED_TOPICS + "=test-.*&" +
                CruiseControlParameters.GOALS + "=";

        StringBuilder goalStringBuilder = new StringBuilder();
        for (int i = 0; i < GOALS.size(); i++) {
            goalStringBuilder.append(GOALS.get(i));
            if (i < GOALS.size() - 1) {
                goalStringBuilder.append(",");
            }
        }

        expectedUrl += URLEncoder.encode(goalStringBuilder.toString(), StandardCharsets.UTF_8) + "&";
        expectedUrl += CruiseControlParameters.REBALANCE_DISK + "=false";
        
        URI uri = new UrlBuilder("localhost", 9090, CruiseControlEndpoints.REBALANCE, true)
            .withParameter(CruiseControlParameters.JSON, "true")
            .withParameter(CruiseControlParameters.DRY_RUN, "false")
            .withParameter(CruiseControlParameters.VERBOSE, "true")
            .withParameter(CruiseControlParameters.SKIP_HARD_GOAL_CHECK, "false")
            .withParameter(CruiseControlParameters.EXCLUDED_TOPICS, "test-.*")
            .withParameter(CruiseControlParameters.GOALS, GOALS)
            .withParameter(CruiseControlParameters.REBALANCE_DISK, "false")
            .build();
        assertThat(uri.toString(), is(expectedUrl));
    }
}
