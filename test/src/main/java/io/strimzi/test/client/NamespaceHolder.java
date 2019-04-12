package io.strimzi.test.client;

public class NamespaceHolder {

    private String namespace;


    private static NamespaceHolder holder = new NamespaceHolder();

    private NamespaceHolder() {}
    public static NamespaceHolder getInstance() {
        return holder;
    }

    public String setNamespace(String namespace) {
        String previousNamespace = namespace;
        this.namespace = namespace;
        return previousNamespace;
    }

    public String getNamespace() {
        return this.namespace;
    }
}
