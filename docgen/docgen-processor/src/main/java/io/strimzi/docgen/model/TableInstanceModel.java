/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.model;

import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.ExecutableElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A model of a table instance
 */
public class TableInstanceModel {

    private final TableKindModel kindModel;

    private final List<List<String>> rows;

    public TableInstanceModel(TableKindModel kindModel) {
        this.kindModel = kindModel;
        this.rows = new ArrayList<>();
    }

    public TableKindModel kindModel() {
        return kindModel;
    }

    public void addRow(List<String> row) {
        rows.add(row);
    }

    public List<List<String>> rows() {
        return rows;
    }

}
