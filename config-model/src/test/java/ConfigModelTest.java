/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.Type;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConfigModelTest {

    @Test
    public void testStringValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.STRING);
        assertThat(cm.validate("test",  "dog"), is(emptyList()));
        cm.setValues(asList("foo", "bar"));
        assertThat(cm.validate("test",  "foo"), is(emptyList()));
        assertThat(cm.validate("test",  "bar"), is(emptyList()));
        assertThat(cm.validate("test",  "baz"),
                is(singletonList("test has value 'baz' which is not one of the allowed values: [foo, bar]")));
        cm.setValues(null);
        cm.setPattern("foo|bar");
        assertThat(cm.validate("test",  "foo"), is(emptyList()));
        assertThat(cm.validate("test",  "bar"), is(emptyList()));
        assertThat(cm.validate("test",  "baz"),
                is(singletonList("test has value 'baz' which does not match the required pattern: foo|bar")));
    }

    @Test
    public void testBooleanValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.BOOLEAN);
        assertThat(cm.validate("test",  "true"), is(emptyList()));
        assertThat(cm.validate("test",  "false"), is(emptyList()));
        assertThat(cm.validate("test",  "dog"), is(singletonList("test has value 'dog' which is not a boolean")));
    }

    @Test
    public void testShortValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.SHORT);
        assertThat(cm.validate("test",  "1"), is(emptyList()));
        assertThat(cm.validate("test",  Short.valueOf(Short.MAX_VALUE).toString()), is(emptyList()));
        assertThat(cm.validate("test",  Short.valueOf(Short.MIN_VALUE).toString()), is(emptyList()));
        assertThat(cm.validate("test", Integer.valueOf((int) Short.MAX_VALUE + 1).toString()), is(singletonList("test has value '32768' which is not a short")));
        assertThat(cm.validate("test", Integer.valueOf((int) Short.MIN_VALUE - 1).toString()), is(singletonList("test has value '-32769' which is not a short")));
        cm.setMinimum(0);
        assertThat(cm.validate("test", "-1"), is(singletonList("test has value -1 which less than the minimum value 0")));
        cm.setMaximum(1);
        assertThat(cm.validate("test", "2"), is(singletonList("test has value 2 which greater than the maximum value 1")));
    }

    @Test
    public void testIntValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.INT);
        assertThat(cm.validate("test", "1"), is(emptyList()));
        assertThat(cm.validate("test", Integer.valueOf(Integer.MAX_VALUE).toString()), is(emptyList()));
        assertThat(cm.validate("test", Integer.valueOf(Integer.MIN_VALUE).toString()), is(emptyList()));
        assertThat(cm.validate("test", Long.valueOf((long) Integer.MAX_VALUE + 1L).toString()), is(singletonList("test has value '2147483648' which is not an int")));
        assertThat(cm.validate("test", Long.valueOf((long) Integer.MIN_VALUE - 1L).toString()), is(singletonList("test has value '-2147483649' which is not an int")));
        cm.setMinimum(0);
        assertThat(cm.validate("test", "-1"), is(singletonList("test has value -1 which less than the minimum value 0")));
        cm.setMaximum(1);
        assertThat(cm.validate("test", "2"), is(singletonList("test has value 2 which greater than the maximum value 1")));
    }

    @Test
    public void testLongValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.LONG);
        assertThat(cm.validate("test", "1"), is(emptyList()));
        assertThat(cm.validate("test", Long.valueOf(Long.MAX_VALUE).toString()), is(emptyList()));
        assertThat(cm.validate("test", Long.valueOf(Long.MIN_VALUE).toString()), is(emptyList()));
        assertThat(cm.validate("test", "9223372036854775808"), is(singletonList("test has value '9223372036854775808' which is not a long")));
        assertThat(cm.validate("test", "-9223372036854775809"), is(singletonList("test has value '-9223372036854775809' which is not a long")));
        cm.setMinimum(0);
        assertThat(cm.validate("test", "-1"), is(singletonList("test has value -1 which less than the minimum value 0")));
        cm.setMaximum(1);
        assertThat(cm.validate("test", "2"), is(singletonList("test has value 2 which greater than the maximum value 1")));
    }

    @Test
    public void testDoubleValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.DOUBLE);
        assertThat(cm.validate("test", "1"), is(emptyList()));
        assertThat(cm.validate("test", "dog"), is(singletonList("test has value 'dog' which is not a double")));
        cm.setMinimum(0.0);
        assertThat(cm.validate("test", "-0.1"), is(singletonList("test has value -0.1 which less than the minimum value 0.0")));
        cm.setMaximum(1.0);
        assertThat(cm.validate("test", "1.1"), is(singletonList("test has value 1.1 which greater than the maximum value 1.0")));
    }

    @Test
    public void testListValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.LIST);
        assertThat(cm.validate("test", "foo"), is(emptyList()));
        assertThat(cm.validate("test", "foo,bar"), is(emptyList()));
        cm.setItems(asList("foo", "bar"));
        assertThat(cm.validate("test", "foo"), is(emptyList()));
        assertThat(cm.validate("test", "foo,bar"), is(emptyList()));
        assertThat(cm.validate("test", "foo,bar,baz"),
                is(singletonList("test contains values [baz] which are not in the allowed items [foo, bar]")));
        assertThat(cm.validate("test", "foo, bar, baz"),
                is(singletonList("test contains values [baz] which are not in the allowed items [foo, bar]")));
        assertThat(cm.validate("test", " foo , bar, baz "),
                is(singletonList("test contains values [baz] which are not in the allowed items [foo, bar]")));
        assertThat(cm.validate("test", " foo , bar,,"),
                is(singletonList("test contains values [] which are not in the allowed items [foo, bar]")));
    }

    @Test
    public void testPasswordValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.PASSWORD);
        assertThat(cm.validate("test", "whatever"), is(emptyList()));
    }

    @Test
    public void testClassValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.CLASS);
        assertThat(cm.validate("test", "org.example.Whatever"), is(emptyList()));
    }
}
