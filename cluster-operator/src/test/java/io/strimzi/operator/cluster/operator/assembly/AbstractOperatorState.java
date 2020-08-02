/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.operator.assembly.KafkaRebalanceState;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AbstractOperatorState {


    public static Matcher<KafkaRebalance> hasState(final KafkaRebalanceState state) {
        return new TypeSafeDiagnosingMatcher<>() {

            @Override
            protected boolean matchesSafely(KafkaRebalance kafkaRebalance, Description mismatchDescription) {
                mismatchDescription.appendText("was ").appendValue(kafkaRebalance);

                if (kafkaRebalance == null) {
                    mismatchDescription.appendText("\n KafkaRebalance is null");
                    return false;
                }

                if (kafkaRebalance.getStatus() == null) {
                    mismatchDescription.appendText("\n KafkaRebalance status is null");
                    return false;
                }

                if (kafkaRebalance.getStatus().getConditions() == null) {
                    mismatchDescription.appendText("\n KafkaRebalance status conditions is null");
                    return false;
                }

                return true;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("Expected state is ").appendValue(state);
            }
        };

    }


    /**
     * Checks all conditions in the supplied status to see if type of one of them matches the supplied rebalance state.
     * @param state he expected rebalance state to be searched for.
     * @return
     */
    public static Matcher<List<Condition>> hasStateInConditions(KafkaRebalanceState state) {
        return new TypeSafeDiagnosingMatcher<>() {

            @Override
            protected boolean matchesSafely(List<Condition> conditions, Description mismatchDescription) {
                mismatchDescription.appendText("was ").appendValue(conditions);

                List<String> foundStatuses = new ArrayList<>();

                for (Condition condition :  conditions) {
                    if (condition == null) {
                        continue;
                    }
                    String type = condition.getType();
                    if (type.equals(state.toString())) {
                        return true;
                    } else {
                        foundStatuses.add(type);
                    }
                }
                mismatchDescription.appendText("\n Condition doesn't have expected value, found " + foundStatuses);
                return false;

            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("Expected values are ").appendValue(state);
            }
        };
    }

    public static Matcher<List<Condition>> hasStateInConditions(KafkaRebalanceState state, Class reason, String message) {
        return new TypeSafeDiagnosingMatcher<>() {

            @Override
            protected boolean matchesSafely(List<Condition> conditions, Description mismatchDescription) {
                mismatchDescription.appendText("was ").appendValue(conditions);

                for (Condition condition: conditions) {

                    if (condition == null) {
                        continue;
                    }

                    if (!is(reason.getSimpleName()).matches(condition.getReason())) {
                        mismatchDescription.appendText("\n KafkaRebalance state condition reason doesn't have expected value, found " + condition.getReason());
                        return false;
                    }

                    if (!containsString(reason.getSimpleName()).matches(condition.getMessage())) {
                        mismatchDescription.appendText("\n KafkaRebalance state condition message doesn't have expected value, found " + condition.getMessage());
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("Expected values are ").appendValue(state).appendValue(reason).appendText(message);
            }
        };
    }

}
