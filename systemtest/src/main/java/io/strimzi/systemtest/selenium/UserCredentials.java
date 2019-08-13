/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.selenium;

public class UserCredentials {
    private String username;
    private String password;

    public UserCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public UserCredentials(String username) {
        this.username = username;
        this.password = username;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public UserCredentials setUsername(String username) {
        this.username = username;
        return this;
    }

    public UserCredentials setPassword(String password) {
        this.password = password;
        return this;
    }

    @Override
    public String toString() {
        return String.format("%s:%s", username, password);
    }
}
