/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class DocGeneratorTest {

    @Test
    void testme() throws IOException {
        DocGenerator.generate(CompletelyUnrelatedClass.class);
    }
}
