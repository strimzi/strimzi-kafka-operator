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

import static io.strimzi.test.Exec.exec;
import static io.strimzi.test.Exec.execOutput;
import static io.strimzi.test.Exec.isExecutableOnPath;

/**
 * A {@link KubeCluster} implementation for {@code minikube} and {@code minishift}.
 */
public class Minikube implements KubeCluster {

    public static final String MINIKUBE = "minikube";
    public static final String MINISHIFT = "minishift";

    private final String cmd;

    private Minikube(String cmd) {
        this.cmd = cmd;
    }

    public static Minikube minikube() {
        return new Minikube(MINIKUBE);
    }

    public static Minikube minishift() {
        return new Minikube(MINISHIFT);
    }

    @Override
    public boolean isAvailable() {
        return isExecutableOnPath(cmd);
    }

    @Override
    public boolean isClusterUp() {
        String output = execOutput(cmd, "status");
        return output.contains("minikube: Running")
                && output.contains("cluster: Running")
                && output.contains("kubectl: Correctly Configured:");
    }

    @Override
    public void clusterUp() {
        exec(cmd, "start");
    }

    @Override
    public void clusterDown() {
        exec(cmd, "stop");
    }

    @Override
    public KubeClient defaultClient() {
        return MINIKUBE.equals(cmd) ? new Kubectl() : new Oc();
    }
}
