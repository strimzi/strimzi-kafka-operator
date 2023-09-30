/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Cruise Control disk capacity
 */
public class DiskCapacity {
    private static final String SINGLE_DISK = "";
    private Map<String, String> map;

    /**
     * Constructor
     */
    public DiskCapacity() {
        map = new HashMap<>(1);
    }

    private DiskCapacity(String size) {
        this();
        map.put(SINGLE_DISK, size);
    }

    protected static DiskCapacity of(String size) {
        return new DiskCapacity(size);
    }

    protected void add(String path, String size) {
        if (path == null || SINGLE_DISK.equals(path)) {
            throw new IllegalArgumentException("The disk path cannot be null or empty");
        }
        map.put(path, size);
    }

    protected Object getJson() {
        if (map.size() == 1 && map.containsKey(SINGLE_DISK)) {
            return map.get(SINGLE_DISK);
        } else {
            JsonObject disks = new JsonObject();
            for (Map.Entry<String, String> e : map.entrySet()) {
                disks.put(e.getKey(), e.getValue());
            }
            return disks;
        }
    }
}