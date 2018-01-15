/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;

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
    public MapName(ConfigMap cm) {
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