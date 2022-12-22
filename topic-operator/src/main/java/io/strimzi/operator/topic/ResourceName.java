/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.HasMetadata;

import java.util.regex.Pattern;

/**
 * Typesafe representation of the name of a K8s resource.
 */
class ResourceName {
    private final String name;

    private static final Pattern RESOURCE_PATTERN = Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*");
    public static final int MAX_RESOURCE_NAME_LENGTH = 253;

    public static boolean isValidResourceName(String resourceName) {
        return resourceName.length() <= MAX_RESOURCE_NAME_LENGTH
                && RESOURCE_PATTERN.matcher(resourceName).matches();
    }

    protected ResourceName(String name) {
        if (!isValidResourceName(name)) {
            throw new IllegalArgumentException("'" + name + "' is not a valid Kubernetes resource name");
        }
        this.name = name;
    }

    /**
     * Create a MapName from the name of the given resource
     *
     * @param resource
     */
    public ResourceName(HasMetadata resource) {
        this(resource.getMetadata().getName());
    }

    /**
     * @return   Resource name
     */
    public String toString() {
        return this.name;
    }

    /**
     * Checks if the resource names are equal or not
     *
     * @param o Object
     * @return Boolean result whether the resource name are equal or not
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ResourceName resourceName = (ResourceName) o;

        return name != null ? name.equals(resourceName.name) : resourceName.name == null;
    }

    /**
     * @return  Hashcode corresponding to the resource name
     */
    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}