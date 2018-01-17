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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class Oc {

    private static final Logger logger = LoggerFactory.getLogger(Oc.class);

    private void exec(String[] cmd) throws IOException, InterruptedException, OcException {
        logger.info("{}", Arrays.toString(cmd));
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.inheritIO();
        Process p = pb.start();
        int sc = p.waitFor();
        if (sc != 0) {
            throw new OcException(sc, Arrays.toString(cmd) + " got status code " + sc);
        }
    }


    public boolean isClusterUp() throws Exception {
        try {
            exec(new String[]{"oc", "cluster", "status"});
            return true;
        } catch (OcException e) {
            if (e.statusCode == 1) {
                return false;
            }
            throw e;
        }
    }

    public void clusterUp() throws Exception {
        exec(new String[]{"oc", "cluster", "up"});
    }

    public void loginSystemAdmin() throws Exception {
        exec(new String[]{"oc", "login", "-u", "system:admin"});
        exec("oc create role strimzi-role --resource=cm --resource=events --verb=get --verb=list --verb=watch --verb=create --verb=patch --verb=update --verb=delete".split(" "));
        exec("oc create rolebinding strimzi-rolebinding --role=strimzi-role --user=developer".split(" "));
    }

    public void loginDeveloper() throws Exception {
        exec(new String[]{"oc", "login", "-u", "developer"});

    }

    public void apply(File file) throws InterruptedException, OcException, IOException {
        if (!file.exists()) {
            throw new RuntimeException("File " + file.getAbsolutePath() + " does not exist");
        }
        exec(new String[]{"oc", "apply", "-f", file.getAbsolutePath()});
    }

    public void clusterDown() throws Exception {
        exec(new String[]{"oc", "cluster", "down"});
    }

    public String masterUrl() {
        return "https://localhost:8443";
    }

}
