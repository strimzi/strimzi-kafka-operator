/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

public class ApiUser {
    private String name;
    private String password;
    private ApiRole role;

    public ApiUser(String name, String password, ApiRole role) {
        this.name = name;
        this.password = password;
        this.role = role;
    }

    public String getName() {
        return name;
    }

    public void setUser(String name) {
        this.name = name;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public ApiRole getRole() {
        return role;
    }

    public void setRole(ApiRole role) {
        this.role = role;
    }
}
