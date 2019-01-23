/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

/**
 * A {@link KubeCluster} implementation using kubectl to already installed clusterCtx
 */
public class KubectlCluster extends BaseKubeCluster {

    public KubectlCluster(String context) {
        super(Kubectl.KUBECTL, context);
    }

    public KubectlCluster() {
        this(null);
    }

    @Override
    public KubeClient defaultClient() {
        return new Kubectl(context);
    }

}
