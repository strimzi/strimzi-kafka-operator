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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static java.lang.String.join;

public abstract class KubeCluster {
    private static final Logger logger = LoggerFactory.getLogger(KubeCluster.class);

    protected void exec(String... cmd) throws KubeClusterException {
        exec(Arrays.asList(cmd));
    }
    protected void exec(List<String> cmd) throws KubeClusterException {
        try {
            logger.info("{}", join(" ", cmd));
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.inheritIO();
            Process p = pb.start();
            int sc = p.waitFor();
            if (sc != 0) {
                throw new KubeClusterException(sc, cmd + " got status code " + sc);
            }
        } catch (IOException e) {
            throw new KubeClusterException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KubeClusterException(e);
        }
    }

    protected String exec2(String... cmd) throws KubeClusterException {

        try {
            logger.info("{}", Arrays.toString(cmd));
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            File tmp = File.createTempFile(KubeCluster.class.getName(), Integer.toString(Arrays.hashCode(cmd)));
            try {
                tmp.deleteOnExit();
                pb.redirectOutput(tmp);
                Process p = pb.start();
                int sc = p.waitFor();
                if (sc != 0) {
                    throw new KubeClusterException(sc, Arrays.toString(cmd) + " got status code " + sc);
                }
                return new String(Files.readAllBytes(tmp.toPath()));
            } finally {
                tmp.delete();
            }
        } catch (IOException e) {
            throw new KubeClusterException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KubeClusterException(e);
        }
    }

    protected boolean isExecutableOnPath(String cmd) {
        for (String dir: System.getenv("PATH").split(Pattern.quote(System.getProperty("path.separator")))) {
            if (new File(dir, cmd).canExecute()) {
                return true;
            }
        }
        return false;
    }

    public abstract boolean isAvailable();

    public abstract boolean isClusterUp();

    public abstract void clusterUp();

    public abstract void clusterDown();

    public abstract void createRole(String roleName, Permission... permissions);

    public abstract void createRoleBinding(String bindingName, String roleName, String... users);

    public abstract void deleteRoleBinding(String bindingName);

    public abstract void deleteRole(String roleName);

    public abstract String defaultNamespace();
}
