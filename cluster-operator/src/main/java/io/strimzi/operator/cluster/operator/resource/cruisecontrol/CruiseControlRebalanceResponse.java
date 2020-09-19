/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.json.JsonObject;

public class CruiseControlRebalanceResponse extends CruiseControlResponse {

    private boolean isNotEnoughDataForProposal;
    private boolean isProposalStillCalaculating;

    CruiseControlRebalanceResponse(String userTaskId, JsonObject json) {
        super(userTaskId, json);
        // There is sufficient data for proposal unless response from Cruise Control says otherwise
        // Sourced from the NotEnoughValidWindows error from the Cruise Control response
        this.isNotEnoughDataForProposal = false;
        // Proposal is not in progress unless response from Cruise Control says otherwise
        // Sourced from the "progress" field in the response with value "proposalStillCalaculating"
        this.isProposalStillCalaculating = false;
    }

    public boolean isNotEnoughDataForProposal() {
        return this.isNotEnoughDataForProposal;
    }

    public void setNotEnoughDataForProposal(boolean notEnoughDataForProposal) {
        this.isNotEnoughDataForProposal = notEnoughDataForProposal;
    }

    public boolean isProposalStillCalaculating() {
        return isProposalStillCalaculating;
    }

    public void setProposalStillCalaculating(boolean proposalStillCalaculating) {
        this.isProposalStillCalaculating = proposalStillCalaculating;
    }
}
