/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

public class NamespaceHolder {

    private static String namespace;

    private NamespaceHolder() {
    }

    static String setNamespaceToHolder(String setNamespace) {
        String previousNamespace = namespace;
        namespace = setNamespace;
        return previousNamespace;
    }

    public static String getNamespaceFromHolder() {
        return namespace;
    }
}
