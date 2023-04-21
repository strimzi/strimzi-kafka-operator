/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtestdoc.markdown;

public class Header {

    public static String firstLevelHeader(String text) {
        return "# " + text;
    }

    public static String secondLevelHeader(String text) {
        return "## " + text;
    }

    public static String thirdLevelHeader(String text) {
        return "### " + text;
    }
}
