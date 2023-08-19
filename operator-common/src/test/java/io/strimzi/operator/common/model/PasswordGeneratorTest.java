/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class PasswordGeneratorTest {

    @Test
    public void length() {
        PasswordGenerator generator = new PasswordGenerator(10, "a", "a");
        assertThat(generator.generate(), is("aaaaaaaaaa"));
    }

    @Test
    public void alphabet() {
        PasswordGenerator generator = new PasswordGenerator(10, "ab", "ab");
        assertThat(generator.generate().matches("[ab]{10}"), is(true));
    }

    @Test
    public void firstLetterAlphabet() {
        PasswordGenerator generator = new PasswordGenerator(10, "a", "b");
        String password = generator.generate();
        assertThat(password.substring(0, 1), is("a"));
        assertThat(password.substring(1, 10), is("bbbbbbbbb"));
    }
}
