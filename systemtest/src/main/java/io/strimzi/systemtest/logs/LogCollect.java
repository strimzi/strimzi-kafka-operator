/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

public interface LogCollect {

    public void collectLogsFromPods();
    public void collectEvents();
    public void collectConfigMaps();
    public void collectDeployments();
    public void collectStatefulSets();
    public void collectReplicaSets();
}
