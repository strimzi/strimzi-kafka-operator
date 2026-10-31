/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import java.time.Instant;
import java.util.List;

/**
 * Information about goal violations detected by Cruise Control.
 *
 * @param detectionTime The time when the goal violations were detected
 * @param fixability The fixability of the detected violations (fixable, unfixable, or mixed)
 * @param fixableGoals List of fixable violated goal names
 * @param unfixableGoals List of unfixable violated goal names
 */
public record GoalViolationInfo(Instant detectionTime, Fixability fixability, List<String> fixableGoals, List<String> unfixableGoals) {

    /**
     * Represents the fixability classification of detected goal violations.
     */
    public enum Fixability {
        /** All violated goals are fixable by a rebalance */
        FIXABLE("fixable"),
        /** All violated goals are unfixable (require manual intervention) */
        UNFIXABLE("unfixable"),
        /** Both fixable and unfixable goal violations are present */
        MIXED("mixed");

        private final String label;

        Fixability(String label) {
            this.label = label;
        }

        /**
         * Returns the label value used for metrics tags.
         *
         * @return the label value used for metrics tags
         */
        public String label() {
            return label;
        }
    }
}
