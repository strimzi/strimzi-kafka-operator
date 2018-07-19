/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.api.kafka.model.KafkaTopic;

import java.util.regex.Pattern;

/**
 * Typesafe representation of the name of a ConfigMap.
 */
class MapName {
    private final String name;

    private static final Pattern RESOURCE_PATTERN = Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*");
    public static final int MAX_RESOURCE_NAME_LENGTH = 253;

    public static boolean isValidResourceName(String resourceName) {
        return resourceName.length() <= MAX_RESOURCE_NAME_LENGTH
                && RESOURCE_PATTERN.matcher(resourceName).matches();
    }

    public MapName(String name) {
        if (!isValidResourceName(name)) {
            throw new IllegalArgumentException("'" + name + "' is not a valid Kubernetes resource name");
        }
        this.name = name;
    }

    /**
     * Create a MapName from the name of the given ConfigMap
     * @param cm
     */
    public MapName(KafkaTopic cm) {
        this(cm.getMetadata().getName());
    }

    public String toString() {
        return this.name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MapName mapName = (MapName) o;

        return name != null ? name.equals(mapName.name) : mapName.name == null;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}