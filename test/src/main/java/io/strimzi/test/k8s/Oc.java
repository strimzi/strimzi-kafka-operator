/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import java.util.List;
import java.util.Map;

/**
 * A {@link KubeClient} implementation wrapping {@code oc}.
 */
public class Oc extends BaseKubeClient<Oc> {

    public static final String OC = "oc";

    public Oc(String ctx) {
        super(ctx, "myproject");
    }

    public Oc() {
        this(null);
    }

    @Override
    public Oc clientWithContext(String context) {
        return new Oc(context);
    }

    public Oc newApp(String template, Map<String, String> params) {
        List<String> cmd = cmdWithContext("new-app", template);
        for (Map.Entry<String, String> entry : params.entrySet()) {
            cmd.add("-p");
            cmd.add(entry.getKey() + "=" + entry.getValue());
        }

        Exec.exec(cmd);
        return this;
    }

    @Override
    protected String cmd() {
        return OC;
    }
}
