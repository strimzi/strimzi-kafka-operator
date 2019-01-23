/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

public class OpenShift extends BaseKubeCluster {

    public OpenShift(String context) {
        super(Oc.OC, context);
    }

    public OpenShift() {
        this(null);
    }

    @Override
    public KubeClient defaultClient() {
        return new Oc(context);
    }
}