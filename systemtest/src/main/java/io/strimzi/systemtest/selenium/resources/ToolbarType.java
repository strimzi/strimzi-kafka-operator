/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.selenium.resources;

public enum ToolbarType {
    ADDRESSES {
        public String toString() {
            return "exampleToolbar";
        }
    },
    CONNECTIONS {
        public String toString() {
            return "connectionToolbar";
        }
    }
}
