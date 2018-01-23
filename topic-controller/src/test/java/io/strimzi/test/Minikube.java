/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link KubeCluster} implementation that uses an already-running cluster,
 * as opposed to starting one itself.
 * This is used in CI environments where the cluster has been started by the CI infrastructure.
 */
public class Minikube extends KubeCluster {

    public static final String MINIKUBE = "minikube";
    public static final String KUBECTL = "kubectl";

    @Override
    public boolean isAvailable() {
        return isExecutableOnPath(MINIKUBE)
                && isExecutableOnPath(KUBECTL);
    }

    @Override
    public boolean isClusterUp() {
        String output = exec2(MINIKUBE, "status");
        return output.contains("minikube: Running")
                && output.contains("cluster: Running")
                && output.contains("kubectl: Correctly Configured:");
    }

    @Override
    public void clusterUp() {
        exec(MINIKUBE, "start");
    }

    @Override
    public void clusterDown() {
        exec(MINIKUBE, "stop");
    }

    // Just have a create and delete

    @Override
    public void createRole(String roleName, Permission... permissions) {
        List<String> cmd = new ArrayList<>();
        cmd.addAll(Arrays.asList(KUBECTL, "create", "role", roleName));
        for (Permission p: permissions) {
            for (String resource: p.resource()) {
                cmd.add("--resource=" + resource);
            }
            for (int i = 0; i < p.verbs().length; i++) {
                cmd.add("--verb="+p.verbs()[i]);
            }
        }
        exec(cmd);
    }

    @Override
    public void createRoleBinding(String bindingName, String roleName, String... users) {
        List<String> cmd = new ArrayList<>();
        cmd.addAll(Arrays.asList(KUBECTL, "create", "rolebinding", bindingName, "--role="+roleName));
        for (int i = 0; i < users.length; i++) {
            cmd.add("--user="+users[i]);
        }
        exec(cmd);
    }

    @Override
    public void deleteRoleBinding(String bindingName) {
        exec(KUBECTL, "delete", "rolebinding", bindingName);
    }

    @Override
    public void deleteRole(String roleName) {
        exec(KUBECTL, "delete", "role", roleName);
    }

    @Override
    public String defaultNamespace() {
        return "default";
    }
}
