/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.vertx.core.Future;

/**
 * Cruise Control REST API interface definition
 */
public interface CruiseControlApi {

    String CC_REST_API_ERROR_KEY = "errorMessage";
    String CC_REST_API_PROGRESS_KEY = "progress";
    String CC_REST_API_USER_ID_HEADER = "User-Task-ID";
    String CC_REST_API_SUMMARY = "summary";

    /**
     *  Gets the state of the Cruise Control server.
     *
     * @param host The address of the Cruise Control server.
     * @param port The port the Cruise Control Server is listening on.
     * @param verbose Whether the response from state endpoint should include more details.
     * @return A future for the response from the Cruise Control server with details of the Cruise Control server state.
     */
    Future<CruiseControlResponse> getCruiseControlState(String host, int port, boolean verbose);

    /**
     *  Send a request to the Cruise Control server to perform a cluster rebalance.
     *
     * @param host The address of the Cruise Control server.
     * @param port The port the Cruise Control Server is listening on.
     * @param options The rebalance parameters to be passed to the Cruise Control server.
     * @param userTaskID This is the unique ID of a previous rebalance request. If a previous request had not been
     *                   completed when the response was returned then this ID can be used to retrieve the results of that
     *                   request.
     * @return A future for the rebalance response from the Cruise Control server containing details of the optimization.
     */
    Future<CruiseControlRebalanceResponse> rebalance(String host, int port, RebalanceOptions options, String userTaskID);

    /**
     *  Get the state of a specific task (e.g. a rebalance) from the Cruise Control server.
     *
     * @param host The address of the Cruise Control server.
     * @param port The port the Cruise Control Server is listening on.
     * @param userTaskID This is the unique ID of a previous rebalance request or other task supported by Cruise Control.
     *                   This is used to retrieve the task's current state.
     * @return A future for the state of the specified task.
     */
    Future<CruiseControlResponse> getUserTaskStatus(String host, int port, String userTaskID);

    /**
     *  Issue a stop command to the Cruise Control server. This will halt any task (e.g. a rebalance) which is currently
     *  in execution.
     *
     * @param host The address of the Cruise Control server.
     * @param port The port the Cruise Control Server is listening on.
     * @return A furture for the response from the Cruise Control server indicating if the stop command was issued.
     */
    Future<CruiseControlResponse> stopExecution(String host, int port);

}

