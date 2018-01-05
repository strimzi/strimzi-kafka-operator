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

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * The entry-point to the topic controller.
 * Main responsibility is to deploy a {@link Session} with an appropriate Config and KubeClient,
 * redeploying if the config changes.
 */
public class Main {

    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static final String ENV_VAR_MASTER_URL = "CONTROLLER_K8S_URL";
    public static final String ENV_VAR_CONFIG_NS = "CONTROLLER_K8S_NS";
    public static final String ENV_VAR_CONFIG_NAME = "CONTROLLER_K8S_NAME";
    public static final String DEFAULT_MASTER_URL = null;
    public static final String DEFAULT_CONFIG_NS = null;
    public static final String DEFAULT_CONFIG_NAME = "topic-controller";

    private Config config;
    private DefaultKubernetesClient kubeClient;
    private Session session;
    private Vertx vertx;

    public String masterUrl;

    public String configNamespace;

    public String configName;

    private Watch controllerConfigWatch;

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        main.masterUrl = getOption(ENV_VAR_MASTER_URL, null);
        main.configNamespace = getOption(ENV_VAR_CONFIG_NS, DEFAULT_CONFIG_NS);
        main.configName = getOption(ENV_VAR_CONFIG_NAME, DEFAULT_CONFIG_NAME);
        main.run();
    }

    private static String getOption(String envVar, String defaultValue) {
        String optionValue = System.getenv(envVar);
        if (optionValue == null) {
            logger.trace("Env var {} is null, using default value: {}", envVar, defaultValue);
            optionValue = defaultValue;
        }
        return optionValue;
    }

    public void run() throws Exception {

        if (this.configName == null
                || this.configName.isEmpty()) {
            throw new IllegalArgumentException("Missing required env var " + ENV_VAR_CONFIG_NAME);
        }
        BootstrapResult bootstrapResult = bootstrap(this.masterUrl, this.configNamespace, this.configName);
        this.configNamespace = bootstrapResult.kubeClient.getNamespace();
        this.vertx = Vertx.vertx();
        vertx.runOnContext(ar-> {
            deployOnContext(bootstrapResult);
        });
    }

    /**
     * Deploy a session, based on the given BootstrapResult.
     * <strong>This should only be called from the vertx context thread, since it mutates the kubeClient and config members.</strong>
     */
    private void deployOnContext(BootstrapResult bootstrapResult) {
        this.kubeClient = bootstrapResult.kubeClient;
        this.config = bootstrapResult.config;
        StringBuilder sb = new StringBuilder(System.lineSeparator());
        for (Config.Value v: this.config.keys()) {
            sb.append("\t").append(v.key).append(": ").append(config.get(v)).append(System.lineSeparator());
        }
        logger.info("Using config:{}", sb.toString());
        this.session = new Session(this.kubeClient, this.config);
        // XXX technically there's a race here, between the deployment and the starting of the watch
        this.vertx.deployVerticle(session, ar -> {
            if (ar.succeeded()) {
                setWatch(bootstrapResult, ar2 -> {});
            }
        });
    }

    private void setWatch(final BootstrapResult bootstrapResult, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(fut -> {
            logger.debug("Setting watch on ConfigMap '{}' in namespace '{}' of apiserver {}", configName, configNamespace, bootstrapResult.masterUrl);
            this.controllerConfigWatch = kubeClient.configMaps().inNamespace(configNamespace == null ? kubeClient.getNamespace() : configNamespace).withName(configName).watch(new Watcher<ConfigMap>() {

                @Override
                public void eventReceived(Action action, ConfigMap cm) {
                    if (cm.getMetadata() != null
                            && configName.equals(cm.getMetadata().getName())
                            && Objects.equals(configNamespace, cm.getMetadata().getNamespace())) {
                        logger.debug("ConfigMap '{}' in namespace '{}' of apiserver {} has been {}",
                                configName, configNamespace, bootstrapResult.masterUrl, action);
                        switch (action) {
                            case DELETED:
                            case ERROR:
                                // Ignore (it might be a delete and recreate
                                break;
                            case MODIFIED:
                            case ADDED:
                                final Config newConfig = new Config(cm.getData());
                                if (!newConfig.equals(Main.this.config)) {
                                    redeploy(newConfig, session.deploymentID(), bootstrapResult.masterUrl);
                                } else {
                                    logger.debug("ConfigMap hasn't actually changed, so ignoring that event");
                                }
                                break;
                        }
                    }
                }

                @Override
                public void onClose(KubernetesClientException cause) {
                    logger.debug("Ceasing watch on ConfigMap '{}' in namespace '{}' of apiserver {}",
                            configName, configNamespace, bootstrapResult);
                }
            });
        },
        handler);
    }

    private void redeploy(Config config, String deploymentID, String bootstrappedUrl) {
        vertx.undeploy(deploymentID, ar -> {
            // Get a local ref to the kubeClient while on the context thread...
            DefaultKubernetesClient kubeClient = this.kubeClient;
            vertx.<BootstrapResult>executeBlocking(fut -> {
                controllerConfigWatch.close();
                // ...but close the kubeClient on the blocking thread
                logger.debug("Stopping kube client");
                kubeClient.close();
                try {
                    fut.complete(bootstrap(config.get(Config.KUBERNETES_MASTER_URL),
                            configNamespace, configName));
                } catch (Exception e) {
                    logger.error("Error while redeploying due to change in ConfigMap '{}' in " +
                                    "namespace '{}' of apiserver {}",
                            configName, configNamespace, bootstrappedUrl, e);
                    fut.fail(e);
                }
            },
            ar2 -> {
                deployOnContext(ar2.result());
            });
        });
    }

    static class BootstrapResult {
        final DefaultKubernetesClient kubeClient;
        final Config config;
        final String masterUrl;

        public BootstrapResult(DefaultKubernetesClient kubeClient, Config config, String masterUrl) {
            this.kubeClient = kubeClient;
            this.config = config;
            this.masterUrl = masterUrl;
        }
    }

    private static BootstrapResult bootstrap(final String initialMasterUrl, String configNamespace, String configName) throws Exception {
        DefaultKubernetesClient kubeClient;
        Config config;
        logger.info("Bootstrapping");
        String currentMasterUrl = initialMasterUrl;
        int bootstrapConnections = 3;
        while (true) {
            logger.info("Connecting to apiserver {}", currentMasterUrl);
            kubeClient = new DefaultKubernetesClient();
            ConfigMap cm = (configNamespace != null ? kubeClient.configMaps().inNamespace(configNamespace) : kubeClient.configMaps()).withName(configName).get();
            if (cm == null) {
                throw new Exception("ConfigMap '" + configName + "' in namespace '" + configNamespace
                        + "' on apiserver " + currentMasterUrl + " does not exist");
            }
            config = new Config(cm.getData());
            final String otherMaster = config.get(Config.KUBERNETES_MASTER_URL);
            /*if ((otherMaster == null && currentMasterUrl != null) || !otherMaster.equals(currentMasterUrl)) {
                logger.info("ConfigMap at apiserver {} nominates a different master {}", currentMasterUrl, otherMaster);
                // Allow the ConfigMap to nominate another master url...
                kubeClient.close();
                currentMasterUrl = otherMaster;
                if (bootstrapConnections--  <= 0) {
                    // ... but only up to a point
                    throw new Exception("Wild goose chase following " + Config.KUBERNETES_MASTER_URL.key
                            + " configs. " + "Started from " + initialMasterUrl);
                }
                continue;
            }*/
            break;
        }
        return new BootstrapResult(kubeClient, config, currentMasterUrl);
    }

}
