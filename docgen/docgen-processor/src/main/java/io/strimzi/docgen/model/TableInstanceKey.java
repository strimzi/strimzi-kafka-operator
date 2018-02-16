/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.model;

import java.util.Objects;

/**
 * Identifies a table instance, based on the kind of table model and a key (which defaults to the name of the class
 * containing the annotated element).
 */
public class TableInstanceKey {
   private final String key;
   private final TableKindModel kind;

    public TableInstanceKey(TableKindModel kind, String key) {
        this.key = key;
        this.kind = kind;
    }

    public String key() {
        return key;
    }

    public TableKindModel kindModel() {
        return kind;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableInstanceKey that = (TableInstanceKey) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(kind, that.kind);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, kind);
    }
}
