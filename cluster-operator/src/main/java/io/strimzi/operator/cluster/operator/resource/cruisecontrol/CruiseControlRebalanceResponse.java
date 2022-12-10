/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.json.JsonObject;

/**
 * Response to rebalance request
 */
public class CruiseControlRebalanceResponse extends CruiseControlResponse {
    private boolean isNotEnoughDataForProposal;
    private boolean isProposalStillCalculating;

    /**
     * Cosntructor
     *
     * @param userTaskId    User task ID
     * @param json          JSON data
     */
    CruiseControlRebalanceResponse(String userTaskId, JsonObject json) {
        super(userTaskId, json);
        // There is sufficient data for proposal unless response from Cruise Control says otherwise
        // Sourced from the NotEnoughValidWindows error from the Cruise Control response
        this.isNotEnoughDataForProposal = false;
        // Proposal is not in progress unless response from Cruise Control says otherwise
        // Sourced from the "progress" field in the response with value "proposalStillCalaculating"
        this.isProposalStillCalculating = false;
    }

    /**
     * @return  True if there is not enough data to generate a proposal. False otherwise.
     */
    public boolean isNotEnoughDataForProposal() {
        return this.isNotEnoughDataForProposal;
    }

    protected void setNotEnoughDataForProposal(boolean notEnoughDataForProposal) {
        this.isNotEnoughDataForProposal = notEnoughDataForProposal;
    }

    /**
     * @return  True if the proposal is still being calculated. False otherwise.
     */
    public boolean isProposalStillCalculating() {
        return isProposalStillCalculating;
    }

    protected void setProposalStillCalculating(boolean proposalStillCalculating) {
        this.isProposalStillCalculating = proposalStillCalculating;
    }
}
