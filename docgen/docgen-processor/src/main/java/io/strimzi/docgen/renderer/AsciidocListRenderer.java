/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.renderer;

import io.strimzi.docgen.model.TableInstanceModel;

import java.io.IOException;
import java.util.List;

public class AsciidocListRenderer implements Renderer {
    @Override
    public String fileFxtension() {
        return "adoc";
    }

    @Override
    public void render(TableInstanceModel instance, Appendable writer) throws IOException {
        for (List<String> row : instance.rows()) {
            writer.append(row.get(0)).append("::").append(System.lineSeparator());
            for (String value : row.subList(1, row.size())) {
                writer.append("  ").append(value).append(System.lineSeparator()).append(System.lineSeparator());
            }
        }
    }
}
