/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.renderer;

import io.strimzi.docgen.model.TableInstanceModel;

import java.io.IOException;

/**
 * Abstracts different output formats.
 */
public interface Renderer {

    /** The file extension without the initial dot */
    String fileFxtension();

    /** Render the table to the given writer */
    void render(TableInstanceModel instance, Appendable writer) throws IOException;
}
