/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.Type;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class ConfigModelTest {

    @Test
    public void testStringValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.STRING);
        assertEquals(emptyList(), cm.validate("test",  "dog"));
        cm.setValues(asList("foo", "bar"));
        assertEquals(emptyList(), cm.validate("test",  "foo"));
        assertEquals(emptyList(), cm.validate("test",  "bar"));
        assertEquals(singletonList("test has value 'baz' which is not one of the allowed values: [foo, bar]"),
                cm.validate("test",  "baz"));
        cm.setValues(null);
        cm.setPattern("foo|bar");
        assertEquals(emptyList(), cm.validate("test",  "foo"));
        assertEquals(emptyList(), cm.validate("test",  "bar"));
        assertEquals(singletonList("test has value 'baz' which does not match the required pattern: foo|bar"),
                cm.validate("test",  "baz"));
    }

    @Test
    public void testBooleanValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.BOOLEAN);
        assertEquals(emptyList(), cm.validate("test",  "true"));
        assertEquals(emptyList(), cm.validate("test",  "false"));
        assertEquals(singletonList("test has value 'dog' which is not a boolean"), cm.validate("test",  "dog"));
    }

    @Test
    public void testShortValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.SHORT);
        assertEquals(emptyList(), cm.validate("test",  "1"));
        assertEquals(emptyList(), cm.validate("test",  Short.valueOf(Short.MAX_VALUE).toString()));
        assertEquals(emptyList(), cm.validate("test",  Short.valueOf(Short.MIN_VALUE).toString()));
        assertEquals(singletonList("test has value '32768' which is not a short"), cm.validate("test", Integer.valueOf((int) Short.MAX_VALUE + 1).toString()));
        assertEquals(singletonList("test has value '-32769' which is not a short"), cm.validate("test", Integer.valueOf((int) Short.MIN_VALUE - 1).toString()));
        cm.setMinimum(0);
        assertEquals(singletonList("test has value -1 which less than the minimum value 0"), cm.validate("test", "-1"));
        cm.setMaximum(1);
        assertEquals(singletonList("test has value 2 which greater than the maximum value 1"), cm.validate("test", "2"));
    }

    @Test
    public void testIntValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.INT);
        assertEquals(emptyList(), cm.validate("test", "1"));
        assertEquals(emptyList(), cm.validate("test", Integer.valueOf(Integer.MAX_VALUE).toString()));
        assertEquals(emptyList(), cm.validate("test", Integer.valueOf(Integer.MIN_VALUE).toString()));
        assertEquals(singletonList("test has value '2147483648' which is not an int"), cm.validate("test", Long.valueOf((long) Integer.MAX_VALUE + 1L).toString()));
        assertEquals(singletonList("test has value '-2147483649' which is not an int"), cm.validate("test", Long.valueOf((long) Integer.MIN_VALUE - 1L).toString()));
        cm.setMinimum(0);
        assertEquals(singletonList("test has value -1 which less than the minimum value 0"), cm.validate("test", "-1"));
        cm.setMaximum(1);
        assertEquals(singletonList("test has value 2 which greater than the maximum value 1"), cm.validate("test", "2"));
    }

    @Test
    public void testLongValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.LONG);
        assertEquals(emptyList(), cm.validate("test", "1"));
        assertEquals(emptyList(), cm.validate("test", Long.valueOf(Long.MAX_VALUE).toString()));
        assertEquals(emptyList(), cm.validate("test", Long.valueOf(Long.MIN_VALUE).toString()));
        assertEquals(singletonList("test has value '9223372036854775808' which is not a long"), cm.validate("test", "9223372036854775808"));
        assertEquals(singletonList("test has value '-9223372036854775809' which is not a long"), cm.validate("test", "-9223372036854775809"));
        cm.setMinimum(0);
        assertEquals(singletonList("test has value -1 which less than the minimum value 0"), cm.validate("test", "-1"));
        cm.setMaximum(1);
        assertEquals(singletonList("test has value 2 which greater than the maximum value 1"), cm.validate("test", "2"));
    }

    @Test
    public void testDoubleValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.DOUBLE);
        assertEquals(emptyList(), cm.validate("test", "1"));
        assertEquals(singletonList("test has value 'dog' which is not a double"), cm.validate("test", "dog"));
        cm.setMinimum(0.0);
        assertEquals(singletonList("test has value -0.1 which less than the minimum value 0.0"), cm.validate("test", "-0.1"));
        cm.setMaximum(1.0);
        assertEquals(singletonList("test has value 1.1 which greater than the maximum value 1.0"), cm.validate("test", "1.1"));
    }

    @Test
    public void testListValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.LIST);
        assertEquals(emptyList(), cm.validate("test", "foo"));
        assertEquals(emptyList(), cm.validate("test", "foo,bar"));
        cm.setItems(asList("foo", "bar"));
        assertEquals(emptyList(), cm.validate("test", "foo"));
        assertEquals(emptyList(), cm.validate("test", "foo,bar"));
        assertEquals(singletonList("test contains values [baz] which are not in the allowed items [foo, bar]"),
                cm.validate("test", "foo,bar,baz"));
        assertEquals(singletonList("test contains values [baz] which are not in the allowed items [foo, bar]"),
                cm.validate("test", "foo, bar, baz"));
        assertEquals(singletonList("test contains values [baz] which are not in the allowed items [foo, bar]"),
                cm.validate("test", " foo , bar, baz "));
        assertEquals(singletonList("test contains values [] which are not in the allowed items [foo, bar]"),
                cm.validate("test", " foo , bar,,"));
    }

    @Test
    public void testPasswordValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.PASSWORD);
        assertEquals(emptyList(), cm.validate("test", "whatever"));
    }

    @Test
    public void testClassValidation() {
        ConfigModel cm = new ConfigModel();
        cm.setType(Type.CLASS);
        assertEquals(emptyList(), cm.validate("test", "org.example.Whatever"));
    }
}
