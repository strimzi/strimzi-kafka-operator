/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test_client;


public class Main {

    public static void main(String[] args) {
        HttpClientsListener server = new HttpClientsListener();
        server.start();
    }
}
