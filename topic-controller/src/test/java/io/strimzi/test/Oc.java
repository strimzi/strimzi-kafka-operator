/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.strimzi.test.Exec.isExecutableOnPath;
import static io.strimzi.test.Exec.exec;

/**
 * A {@link KubeClient} implementation wrapping {@code oc}.
 */
public class Oc implements KubeClient {

    public static final String OC = "oc";

    @Override
    public boolean clientAvailable() {
        return isExecutableOnPath(OC);
    }

    @Override
    public void createRole(String roleName, Permission... permissions) {
        exec(OC, "login", "-u", "system:admin");
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
        exec(cmd);
        exec(OC, "login", "-u", "developer");
    }

    @Override
    public void createRoleBinding(String bindingName, String roleName, String... user) {
        exec(OC, "login", "-u", "system:admin");
        List<String> cmd = new ArrayList<>();
        cmd.addAll(Arrays.asList(OC, "create", "rolebinding", bindingName, "--role=" + roleName));
        for (int i = 0; i < user.length; i++) {
            cmd.add("--user=" + user[i]);
        }
        exec(cmd);
        exec(OC, "login", "-u", "developer");
    }

    @Override
    public void deleteRoleBinding(String bindingName) {
        exec(OC, "login", "-u", "system:admin");
        exec(OC, "delete", "rolebinding", bindingName);
        exec(OC, "login", "-u", "developer");
    }

    @Override
    public void deleteRole(String roleName) {
        exec(OC, "login", "-u", "system:admin");
        exec(OC, "delete", "role", roleName);
        exec(OC, "login", "-u", "developer");
    }

    @Override
    public String defaultNamespace() {
        return "myproject";
    }

}
