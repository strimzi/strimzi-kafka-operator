/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.strimzi.test.k8s.Exec.exec;

/**
 * A {@link KubeClient} wrapping {@code kubectl}.
 */
public class Kubectl implements KubeClient {

    public static final String KUBECTL = "kubectl";

    @Override
    public boolean clientAvailable() {
        return Exec.isExecutableOnPath(KUBECTL);
    }

    @Override
    public void createRole(String roleName, Permission... permissions) {
        List<String> cmd = new ArrayList<>();
        cmd.addAll(Arrays.asList(KUBECTL, "create", "role", roleName));
        for (Permission p: permissions) {
            for (String resource: p.resource()) {
                cmd.add("--resource=" + resource);
            }
            for (int i = 0; i < p.verbs().length; i++) {
                cmd.add("--verb=" + p.verbs()[i]);
            }
        }
        Exec.exec(cmd);
    }

    @Override
    public void createRoleBinding(String bindingName, String roleName, String... users) {
        List<String> cmd = new ArrayList<>();
        cmd.addAll(Arrays.asList(KUBECTL, "create", "rolebinding", bindingName, "--role=" + roleName));
        for (int i = 0; i < users.length; i++) {
            cmd.add("--user=" + users[i]);
        }
        Exec.exec(cmd);
    }

    @Override
    public void deleteRoleBinding(String bindingName) {
        Exec.exec(KUBECTL, "delete", "rolebinding", bindingName);
    }

    @Override
    public void deleteRole(String roleName) {
        Exec.exec(KUBECTL, "delete", "role", roleName);
    }

    @Override
    public String defaultNamespace() {
        return "default";
    }
}
