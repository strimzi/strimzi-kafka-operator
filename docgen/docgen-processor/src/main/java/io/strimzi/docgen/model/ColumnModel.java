/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.model;

import io.strimzi.docgen.annotations.Table;

import javax.lang.model.element.Element;

public class ColumnModel {
    private final String elementSimpleName;
    private final String heading;

    public ColumnModel(Element element, Table.Column columnAnnotation) {
        this.elementSimpleName = element.getSimpleName().toString();
        this.heading = columnAnnotation.heading();
    }

    public String heading() {
        return heading;
    }

    public String elementName() {
        return elementSimpleName;
    }
}
