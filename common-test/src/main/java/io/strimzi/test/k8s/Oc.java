/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;

/**
 * A {@link KubeClient} implementation wrapping {@code oc}.
 */
public class Oc extends BaseKubeClient<Oc> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Oc.class);

    private static final String OC = "oc";

    public Oc() {

    }

    @Override
    protected Context adminContext() {
        String previous = Exec.exec(Oc.OC, "whoami").out().trim();
        String admin = "system:admin";
        LOGGER.trace("Switching from login {} to {}", previous, admin);
        Exec.exec(Oc.OC, "login", "-u", admin);
        return new Context() {
            @Override
            public void close() {
                LOGGER.trace("Switching back to login {} from {}", previous, admin);
                Exec.exec(Oc.OC, "login", "-u", previous, "-p", "foo");
            }
        };
    }

    @Override
    public Oc clientWithAdmin() {
        return new Oc() {

            @Override
            protected Context defaultContext() {
                return adminContext();
            }

            @Override
            public Oc clientWithAdmin() {
                return this;
            }
        };
    }

    @Override
    public String defaultNamespace() {
        return "myproject";
    }

    @Override
    public Oc createNamespace(String name) {
        try (Context context = defaultContext()) {
            Exec.exec(cmd(), "new-project", name);
        }
        return this;
    }

    public Oc newApp(String template) {
        return newApp(template, emptyMap());
    }

    public Oc newApp(String template, Map<String, String> params) {
        List<String> cmd = new ArrayList<>(params.size() + 3);
        cmd.addAll(asList(OC, "new-app", template));
        for (Map.Entry<String, String> entry : params.entrySet()) {
            cmd.add("-p");
            cmd.add(entry.getKey() + "=" + entry.getValue());
        }

        Exec.exec(cmd);
        return this;
    }

    public Oc newProject(String name) {
        Exec.exec(OC, "new-project", name);
        return this;
    }

    @Override
    protected String cmd() {
        return OC;
    }
}
