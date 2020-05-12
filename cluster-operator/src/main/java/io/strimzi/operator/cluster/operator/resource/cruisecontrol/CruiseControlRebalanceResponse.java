/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.json.JsonObject;

public class CruiseControlRebalanceResponse extends CruiseControlResponse {

    private boolean notEnoughDataForProposal;
    private boolean proposalIsStillCalculating;

    CruiseControlRebalanceResponse(String userTaskId, JsonObject json) {
        super(userTaskId, json);
        this.notEnoughDataForProposal = false;
        this.proposalIsStillCalculating = false;
    }

    public boolean thereIsNotEnoughDataForProposal() {
        return this.notEnoughDataForProposal;
    }

    public void setNotEnoughDataForProposal(boolean notEnoughData) {
        this.notEnoughDataForProposal = notEnoughData;
    }

    public boolean proposalIsStillCalculating() {
        return proposalIsStillCalculating;
    }

    public void setProposalIsStillCalculating(boolean proposalIsStillCalculating) {
        this.proposalIsStillCalculating = proposalIsStillCalculating;
    }
}
