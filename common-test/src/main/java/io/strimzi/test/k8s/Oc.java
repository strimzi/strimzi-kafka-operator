/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link KubeClient} implementation wrapping {@code oc}.
 */
public class Oc implements KubeClient {

    public static final String OC = "oc";

    @Override
    public boolean clientAvailable() {
        return Exec.isExecutableOnPath(OC);
    }

    @Override
    public void createRole(String roleName, Permission... permissions) {
        Exec.exec(OC, "login", "-u", "system:admin");
        List<String> cmd = new ArrayList<>();
        cmd.addAll(Arrays.asList(OC, "create", "role", roleName));
        for (Permission p: permissions) {
            for (String resource: p.resource()) {
                cmd.add("--resource=" + resource);
            }
            for (int i = 0; i < p.verbs().length; i++) {
                cmd.add("--verb=" + p.verbs()[i]);
            }
        }
        Exec.exec(cmd);
        Exec.exec(OC, "login", "-u", "developer");
    }

    @Override
    public void createRoleBinding(String bindingName, String roleName, String... user) {
        Exec.exec(OC, "login", "-u", "system:admin");
        List<String> cmd = new ArrayList<>();
        cmd.addAll(Arrays.asList(OC, "create", "rolebinding", bindingName, "--role=" + roleName));
        for (int i = 0; i < user.length; i++) {
            cmd.add("--user=" + user[i]);
        }
        Exec.exec(cmd);
        Exec.exec(OC, "login", "-u", "developer");
    }

    @Override
    public void deleteRoleBinding(String bindingName) {
        Exec.exec(OC, "login", "-u", "system:admin");
        Exec.exec(OC, "delete", "rolebinding", bindingName);
        Exec.exec(OC, "login", "-u", "developer");
    }

    @Override
    public void deleteRole(String roleName) {
        Exec.exec(OC, "login", "-u", "system:admin");
        Exec.exec(OC, "delete", "role", roleName);
        Exec.exec(OC, "login", "-u", "developer");
    }

    @Override
    public String defaultNamespace() {
        return "myproject";
    }

}
