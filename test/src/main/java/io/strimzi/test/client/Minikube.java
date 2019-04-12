/*
 * Copyright 2017-2018, EnMasse authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.client;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.strimzi.test.Environment;

public class Minikube extends Kubernetes {

    protected Minikube(Environment environment, String defaultNamespace) {
        super(environment, new DefaultKubernetesClient(), defaultNamespace);
    }
}
