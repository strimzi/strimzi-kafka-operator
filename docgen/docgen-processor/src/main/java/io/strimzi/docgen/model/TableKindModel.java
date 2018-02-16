/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.model;

import io.strimzi.docgen.annotations.Table;

import javax.lang.model.element.TypeElement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * A model of a particular kind of table
 */
public class TableKindModel {

    private final List<ColumnModel> columns;
    private final TypeElement annotationType;

    public TableKindModel(TypeElement annotationType, List<ColumnModel> columns) {
        this.annotationType = annotationType;
        this.columns = columns;
    }

    public TypeElement typeElement() {
        return annotationType;
    }

    public Collection<ColumnModel> columns() {
        return columns;
    }

    public List<String> columnHeadings() {
        List<String> result = new ArrayList<>(columns.size());
        for (ColumnModel c : columns) {
            result.add(c.heading());
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableKindModel that = (TableKindModel) o;
        return Objects.equals(annotationType, that.annotationType);
    }

    @Override
    public int hashCode() {

        return Objects.hash(annotationType);
    }
}
