/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

/**
 * A {@link KubeClient} wrapping {@code kubectl}.
 */
public class Kubectl extends BaseKubeClient<Kubectl> {

    public static final String KUBECTL = "kubectl";

    Kubectl() {

    }

    Kubectl(String futureNamespace) {
        namespace = futureNamespace;
    }

    @Override
    public Kubectl namespace(String namespace) {
        return new Kubectl(namespace);
    }

    @Override
    public String namespace() {
        return namespace;
    }


    @Override
    public String defaultNamespace() {
        return "default";
    }

    @Override
    protected String cmd() {
        return KUBECTL;
    }

    @Override
    public Kubectl clientWithAdmin() {
        return this;
    }
}
