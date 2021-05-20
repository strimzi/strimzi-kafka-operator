/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

public interface LogCollect {

    public void collectLogsFromPods(String namespaceName);
    public void collectEvents(String namespaceName);
    public void collectConfigMaps(String namespaceName);
    public void collectDeployments(String namespaceName);
    public void collectStatefulSets(String namespaceName);
    public void collectReplicaSets(String namespaceName);
}
