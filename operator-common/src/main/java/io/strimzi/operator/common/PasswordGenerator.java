/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import java.security.SecureRandom;

/**
 * Class for generating passwords for SASL users
 */
public class PasswordGenerator {
    private final SecureRandom rng = new SecureRandom();
    private final int length;
    private final String firstCharacterAlphabet;
    private final String alphabet;


    /**
     * Constructor to initialize the PasswordGenerator with alphabets and password length.
     * It distinguishes between the alphabet for the first character of the password and the rest.
     * This is needed because of how Java JAAS parses the configuration string.
     *
     * @param length    The length of the generated passwords
     * @param firstCharacterAlphabet    A String with characters which will be used to generate the first character of the password
     * @param alphabet  A String with characters which will be used to generate the character of the password starting with the second character
     */
    public PasswordGenerator(int length, String firstCharacterAlphabet, String alphabet) {
        this.length = length;
        this.firstCharacterAlphabet = firstCharacterAlphabet;
        this.alphabet = alphabet;
    }

    public PasswordGenerator(int length) {
        this(length, "abcdefghijklmnopqrstuvwxyz" +
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                "abcdefghijklmnopqrstuvwxyz" +
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                "0123456789");
    }

    /**
     * Generates the password
     *
     * @return String with new password
     */
    public String generate() {
        StringBuilder sb = new StringBuilder();

        sb.append(firstCharacterAlphabet.charAt(rng.nextInt(firstCharacterAlphabet.length())));

        for (int i = 1; i < length; i++) {
            sb.append(alphabet.charAt(rng.nextInt(alphabet.length())));
        }

        return sb.toString();
    }
}
