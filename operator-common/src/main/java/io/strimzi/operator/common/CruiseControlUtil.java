/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import static java.lang.String.format;
import static java.lang.String.join;

/**
 * Class with various utility methods for Cruise Control.
 */
public class CruiseControlUtil {
    /**
     * Build basic HTTP authentication header value.
     *
     * @param username Username.
     * @param password Password.
     *
     * @return Header value.
     */
    public static String buildBasicAuthValue(String username, String password) {
        String credentials = join(":", username, password);
        return format("Basic %s", Util.encodeToBase64(credentials));
    }
}
