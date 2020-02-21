/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly.cruisecontrol;

import java.util.List;

public class RebalanceOptions {

    private boolean isDryRun;
    private List<String> goals;
    private boolean verbose;
    private final boolean json = true;

    public boolean isDryRun() {
        return isDryRun;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public List<String> getGoals() {
        return goals;
    }

    private RebalanceOptions(RebalanceOptionsBuilder builder) {
        this.isDryRun = builder.isDryRun;
        this.goals = builder.goals;
    }

    public static class RebalanceOptionsBuilder {

        private boolean isDryRun;
        private boolean verbose;
        private List<String> goals;

        public RebalanceOptionsBuilder() {
            isDryRun = true;
            verbose = false;
            goals = null;
        }

        public RebalanceOptionsBuilder withFullRun() {
            this.isDryRun = false;
            return this;
        }

        public RebalanceOptionsBuilder withVerboseResponse() {
            this.verbose = true;
            return this;
        }

        public RebalanceOptionsBuilder withGoals(List<String> goals) {
            this.goals = goals;
            return this;
        }

        public RebalanceOptions build() {
            return new RebalanceOptions(this);
        }



    }

}
