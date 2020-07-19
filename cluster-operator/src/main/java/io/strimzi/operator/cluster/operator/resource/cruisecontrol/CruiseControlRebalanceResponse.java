/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.json.JsonObject;

public class CruiseControlRebalanceResponse extends CruiseControlResponse {

    private boolean isSufficientDataForProposal;
    private boolean isProposalInProgress;

    CruiseControlRebalanceResponse(String userTaskId, JsonObject json) {
        super(userTaskId, json);
        // There is sufficient data for proposal unless response says otherwise
        this.isSufficientDataForProposal = true;
        // Proposal is not in progress unless response says otherwise
        this.isProposalInProgress = false;
    }

    public boolean isSufficientDataForProposal() {
        return this.isSufficientDataForProposal;
    }

    public void setSufficientDataForProposal(boolean notEnoughData) {
        this.isSufficientDataForProposal = notEnoughData;
    }

    public boolean isProposalInProgress() {
        return isProposalInProgress;
    }

    public void setProposalInProgress(boolean proposalInProgress) {
        this.isProposalInProgress = proposalInProgress;
    }
}
