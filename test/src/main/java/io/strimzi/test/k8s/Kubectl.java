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

    public Kubectl(String context) {
        super(context, "default");
    }

    public Kubectl() {
        this(null);
    }

    @Override
    protected String cmd() {
        return KUBECTL;
    }

    @Override
    public Kubectl clientWithContext(String ctx) {
        return new Kubectl(ctx);
    }
}
