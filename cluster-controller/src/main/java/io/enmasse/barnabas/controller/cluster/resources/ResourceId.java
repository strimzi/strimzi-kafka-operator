package io.enmasse.barnabas.controller.cluster.resources;

import java.util.Objects;

public class ResourceId {
    private final String type;
    private final String name;

    public ResourceId(String type, String name) {
        this.type = type;
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ResourceId resourceObj = (ResourceId)obj;

        if (name.equals(resourceObj.getName()) && type.equals(resourceObj.getType())) {
            return true;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }

    @Override
    public String toString() {
        return type + ":" + name;
    }
}
