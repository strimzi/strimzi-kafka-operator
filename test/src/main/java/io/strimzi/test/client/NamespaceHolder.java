package io.strimzi.test.client;

public class NamespaceHolder {

    private static String namespace;

    public static String saveNamespace(String n) {
        String previousNamespace = namespace;
        namespace = n;
        return previousNamespace;
    }

    public static String getNamespace() {
        return namespace;
    }
}
