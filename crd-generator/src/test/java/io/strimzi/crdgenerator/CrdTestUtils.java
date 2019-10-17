/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

class CrdTestUtils {
    private CrdTestUtils() {}

    static String readResource(String resourceName) {
        try {
            return new String(
                    Files.readAllBytes(Paths.get(
                            CrdTestUtils.class.getResource(resourceName).toURI())),
                    StandardCharsets.UTF_8);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
