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

package io.strimzi.controller.topic;

import java.io.IOException;
import java.util.Arrays;

public class Oc {

    public void clusterUp() throws Exception {
        exec(new String[]{"oc", "cluster", "up"});
    }

    private void exec(String[] cmd) throws IOException, InterruptedException, OcException {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        Process p = pb.start();
        int sc = p.waitFor();
        if (sc != 0) {
            throw new OcException(Arrays.toString(cmd) + " got status code " + sc);
        }
    }

    public void clusterDown() throws Exception {
        exec(new String[]{"oc", "cluster", "down"});
    }

    public String masterUrl() {
        return "https://localhost:8443";
    }

}
