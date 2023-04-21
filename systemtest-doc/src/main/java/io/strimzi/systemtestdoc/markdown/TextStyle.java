/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtestdoc.markdown;

public class TextStyle {

    public static String boldText(String text) {
        return "**" + text + "**";
    }

    public static String italicText(String text) {
        return "_" + text + "_";
    }
}
