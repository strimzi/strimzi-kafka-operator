/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.config;

import io.strimzi.operator.common.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigParameterParserTest {
    @Test
    public void testPatternConfig() {
        assertThat(ConfigParameterParser.PATTERN.parse(null), is(nullValue()));
        assertThat(ConfigParameterParser.PATTERN.parse(null), is(nullValue()));
        assertThrows(InvalidConfigurationException.class, () -> ConfigParameterParser.PATTERN.parse("["));

        Pattern validPattern = ConfigParameterParser.PATTERN.parse("^foo.+");
        assertThat(validPattern, is(notNullValue()));
        assertThat(validPattern.pattern(), is("^foo.+"));
        assertThat(validPattern.matcher("foobar").matches(), is(true));
        assertThat(validPattern.matcher("barfoo").matches(), is(false));
    }
}