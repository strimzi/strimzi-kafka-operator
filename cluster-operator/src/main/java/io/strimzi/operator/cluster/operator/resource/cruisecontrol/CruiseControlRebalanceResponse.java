/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.json.JsonObject;

public class CruiseControlRebalanceResponse extends CruiseControlResponse {

    private boolean isNotEnoughDataForProposal;
    private boolean isProposalStillCalculating;
    private boolean isBrokersNotExist;

    CruiseControlRebalanceResponse(String userTaskId, JsonObject json) {
        super(userTaskId, json);
        // There is sufficient data for proposal unless response from Cruise Control says otherwise
        // Sourced from the NotEnoughValidWindows error from the Cruise Control response
        this.isNotEnoughDataForProposal = false;
        // Proposal is not in progress unless response from Cruise Control says otherwise
        // Sourced from the "progress" field in the response with value "proposalStillCalaculating"
        this.isProposalStillCalculating = false;
        // One or more brokers provided during a rebalance with add/remove broker endpoints don't exist
        this.isBrokersNotExist = false;
    }

    public boolean isNotEnoughDataForProposal() {
        return this.isNotEnoughDataForProposal;
    }

    public void setNotEnoughDataForProposal(boolean notEnoughDataForProposal) {
        this.isNotEnoughDataForProposal = notEnoughDataForProposal;
    }

    public boolean isProposalStillCalculating() {
        return isProposalStillCalculating;
    }

    public void setProposalStillCalculating(boolean proposalStillCalculating) {
        this.isProposalStillCalculating = proposalStillCalculating;
    }

    public boolean isBrokersNotExist() {
        return this.isBrokersNotExist;
    }

    public void setBrokersNotExist(boolean brokersNotExist) {
        this.isBrokersNotExist = brokersNotExist;
    }
}
