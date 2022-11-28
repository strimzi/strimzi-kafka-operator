/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

/**
 * Namespace and name holder used to identify a Kubernetes object in a single name
 */
public class NamespaceAndName {
    private final String namespace;
    private final String name;

    /**
     * Constructor
     *
     * @param namespace     Namespace
     * @param name          NAme
     */
    public NamespaceAndName(String namespace, String name)   {
        this.namespace = namespace;
        this.name = name;
    }

    /**
     * @return  Namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * @return  NAme
     */
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (obj instanceof NamespaceAndName) {
            NamespaceAndName nrn = (NamespaceAndName) obj;
            if ((nrn.getName() == null && name == null) ||
                    (nrn.getName().equals(name) && ((nrn.getNamespace() == null && namespace == null)
                            || nrn.getNamespace().equals(namespace)))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = namespace != null ? namespace.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return namespace + "/" + name;
    }
}