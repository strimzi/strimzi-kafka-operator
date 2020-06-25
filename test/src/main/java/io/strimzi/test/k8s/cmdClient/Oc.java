/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cmdClient;

import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * A {@link KubeCmdClient} implementation wrapping {@code oc}.
 */
public class Oc extends BaseCmdKubeClient<Oc> {

    private static final Logger LOGGER = LogManager.getLogger(Oc.class);

    private static final String OC = "oc";

    public Oc() { }

    private Oc(String futureNamespace) {
        namespace = futureNamespace;
    }

    @Override
    protected Context adminContext() {
        String previous = Exec.exec(Oc.OC, "whoami").out().trim();
        String admin = System.getenv().getOrDefault("TEST_CLUSTER_ADMIN", "developer");
        LOGGER.trace("Switching from login {} to {}", previous, admin);
        Exec.exec(Oc.OC, "login", "-u", admin);
        return new Context() {
            @Override
            public void close() {
                LOGGER.trace("Switching back to login {} from {}", previous, admin);
                Exec.exec(Oc.OC, "login", "-u", previous);
            }
        };
    }

    @Override
    public Oc clientWithAdmin() {
        return new AdminOc();
    }

    @Override
    public String defaultNamespace() {
        return "myproject";
    }

    @Override
    public String defaultOlmNamespace() {
        return "openshift-marketplace";
    }

    @Override
    public Oc namespace(String namespace) {
        return new Oc(namespace);
    }

    @Override
    public String namespace() {
        return namespace;
    }

    @Override
    public Oc createNamespace(String name) {
        try (Context context = defaultContext()) {
            Exec.exec(cmd(), "new-project", name);
        }
        return this;
    }

    public Oc newApp(String template, Map<String, String> params) {
        List<String> cmd = namespacedCommand("new-app", template);
        for (Map.Entry<String, String> entry : params.entrySet()) {
            cmd.add("-p");
            cmd.add(entry.getKey() + "=" + entry.getValue());
        }

        Exec.exec(cmd);
        return this;
    }

    @Override
    public String cmd() {
        return OC;
    }

    /**
     * An {@code Oc} which uses the admin context.
     */
    private class AdminOc extends Oc {

        @Override
        public String namespace() {
            return Oc.this.namespace();
        }

        @Override
        protected Context defaultContext() {
            return adminContext();
        }

        @Override
        public Oc clientWithAdmin() {
            return this;
        }
    }
}
