/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import java.security.SecureRandom;

public class PasswordGenerator {

    private final SecureRandom rng = new SecureRandom();
    private final int length;
    private final String alphabet;

    public PasswordGenerator(int length, String alphabet) {
        this.length = length;
        this.alphabet = alphabet;
    }

    public String generate() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(alphabet.charAt(rng.nextInt(alphabet.length())));
        }
        return sb.toString();
    }
}
