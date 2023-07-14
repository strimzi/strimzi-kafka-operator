/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cmdClient;

/**
 * A {@link KubeCmdClient} wrapping {@code kubectl}.
 */
public class Kubectl extends BaseCmdKubeClient<Kubectl> {

    public static final String KUBECTL = "kubectl";

    public Kubectl() { }

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
    public String cmd() {
        return KUBECTL;
    }
}
