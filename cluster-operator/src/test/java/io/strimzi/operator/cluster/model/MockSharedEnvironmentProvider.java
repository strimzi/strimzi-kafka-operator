/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mock shared environment for testing.
 */
public class MockSharedEnvironmentProvider implements SharedEnvironmentProvider {
    private final Map<String, EnvVar> envVarMap;

    /**
     * Creates an empty shared environment provider.
     */
    public MockSharedEnvironmentProvider() {
        this.envVarMap = Collections.emptyMap();
    }

    /**
     * Creates a custom shared environment provider.
     *
     * @param envVarMap Custom env var map.
     */
    public MockSharedEnvironmentProvider(Map<String, EnvVar> envVarMap) {
        List<String> sharedNames = names();
        Map<String, EnvVar> filteredMap = new HashMap<>(envVarMap);
        filteredMap.keySet().removeIf(k -> !sharedNames.contains(k));
        this.envVarMap = Collections.unmodifiableMap(filteredMap);
    }

    @Override
    public List<String> names() {
        return Arrays.stream(EnvVarName.values()).map(Enum::name).toList();
    }

    @Override
    public Collection<EnvVar> variables() {
        return Collections.unmodifiableCollection(envVarMap.values());
    }

    @Override
    public String value(String name) {
        return name != null && envVarMap.get(name) != null
            ? envVarMap.get(name).getValue() : null;
    }
}
