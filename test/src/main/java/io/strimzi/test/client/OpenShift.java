/*
 * Copyright 2017-2018, EnMasse authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.client;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.strimzi.test.Environment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handles interaction with openshift cluster
 */
public class OpenShift extends Kubernetes {
    private static final Logger LOGGER = LogManager.getLogger(OpenShift.class);

    public OpenShift(Environment environment, String defaultNamespace) {
        super(environment, new DefaultOpenShiftClient(new ConfigBuilder().withMasterUrl(environment.getApiUrl())
                .withOauthToken(environment.getApiToken())
                .build()), defaultNamespace);
    }
}
