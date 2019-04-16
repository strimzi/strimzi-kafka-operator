package io.strimzi.test.k8s;

import io.strimzi.test.k8s.BaseKubeClient;

public class NamespaceHolder {

    private static String namespace;

    public String setNamespaceToHolder(String setNamespace) {
        String previousNamespace = namespace;
        namespace = setNamespace;
        return previousNamespace;
    }

    public static String getNamespaceFromHolder() {
        return namespace;
    }
}
