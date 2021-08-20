/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import java.util.Objects;

public class Cmd {

    private final StringBuilder stringBuilder;

    public Cmd(String cmd, String... args) {
        stringBuilder = new StringBuilder();
        append(cmd);
        for (var s : args) {
            append(s);
        }
    }

    public Cmd append(String str) {
        if (Objects.requireNonNull(str).isEmpty()) {
            throw new IllegalArgumentException();
        }
        stringBuilder.append('\'');
        for (var i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (ch == '\'') {
                stringBuilder.append("'\"'\"'");
            } else {
                stringBuilder.append(ch);
            }
        }
        stringBuilder.append('\'').append(' ');
        return this;
    }

    public Cmd appendf(String str, Object... args) {
        return append(String.format(str, args));
    }

    @Override
    public String toString() {
        if (stringBuilder.length() > 0) {
            stringBuilder.setLength(stringBuilder.length() - 1);
        }
        return stringBuilder.toString();
    }
}
