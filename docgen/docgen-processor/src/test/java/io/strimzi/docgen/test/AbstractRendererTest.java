/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.test;

import io.strimzi.docgen.annotations.Table;
import io.strimzi.docgen.model.ColumnModel;
import io.strimzi.docgen.model.TableInstanceModel;
import io.strimzi.docgen.model.TableKindModel;

import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractRendererTest {
    protected TableInstanceModel tableInstance(String... col) {
        List<ColumnModel> c = new ArrayList<>(col.length);
        for (String cx : col) {
            TypeElement columnElement = mock(TypeElement.class);
            Name name = mock(Name.class);
            when(name.toString()).thenReturn(cx);
            when(columnElement.getSimpleName()).thenReturn(name);
            Table.Column columnAnnotation = mock(Table.Column.class);
            when(columnAnnotation.heading()).thenReturn("Column for " + cx);
            c.add(new ColumnModel(columnElement, columnAnnotation));
        }
        TypeElement element = mock(TypeElement.class);
        TableKindModel kind = new TableKindModel(element, c);
        return new TableInstanceModel(kind);
    }
}
