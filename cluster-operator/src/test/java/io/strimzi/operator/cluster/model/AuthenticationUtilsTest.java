/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import org.junit.jupiter.api.Test;

import javax.security.auth.login.Configuration;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AuthenticationUtilsTest {
    @Test
    public void testValidJaasConfig() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");
        options.put("key2", "value2");

        String moduleName = "Module";
        String expected = "Module required key1=\"value1\" key2=\"value2\";";
        assertEquals(expected, AuthenticationUtils.JaasConfig(moduleName, options));
    }

    @Test
    public void testEmptyModuleName() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");

        String moduleName = "";
        assertThrows(IllegalArgumentException.class, () -> AuthenticationUtils.JaasConfig(moduleName, options));
    }

    @Test
    public void testConfigWithNullOptionKey() {
        String moduleName = "ExampleModule";
        Map<String, String> options = new HashMap<>();
        options.put(null, "value1");

        assertThrows(NullPointerException.class, () -> AuthenticationUtils.JaasConfig(moduleName, options));
    }

    @Test
    public void testConfigWithNullOptionValue() {
        String moduleName = "ExampleModule";
        Map<String, String> options = new HashMap<>();
        options.put("option1", null);

        assertThrows(NullPointerException.class, () -> AuthenticationUtils.JaasConfig(moduleName, options));
    }

    @Test
    public void testModuleNameContainsEqualSign() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");

        String moduleName = "Module=";
        assertThrows(IllegalArgumentException.class, () -> AuthenticationUtils.JaasConfig(moduleName, options));
    }

    @Test
    public void testModuleNameContainsSemicolon() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");

        String moduleName = "Module;";
        assertThrows(IllegalArgumentException.class, () -> AuthenticationUtils.JaasConfig(moduleName, options));
    }

    @Test
    public void testKeyContainsEqualSign() {
        Map<String, String> options = new HashMap<>();
        options.put("key1=", "value1");

        String moduleName = "Module";
        assertThrows(IllegalArgumentException.class, () -> AuthenticationUtils.JaasConfig(moduleName, options));
    }

    @Test
    public void testKeyContainsSemicolon() {
        Map<String, String> options = new HashMap<>();
        options.put("key1;", "value1");

        String moduleName = "Module";
        assertThrows(IllegalArgumentException.class, () -> AuthenticationUtils.JaasConfig(moduleName, options));
    }

    @Test
    public void testValueContainsEqualSign() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value=1");

        String moduleName = "Module";
        String expected = "Module required key1=\"value=1\";";
        assertEquals(expected, AuthenticationUtils.JaasConfig(moduleName, options));    }

    @Test
    public void testValueContainsSemicolon() {
        Map<String, String> options = new HashMap<>();
        options.put("key1", ";");

        String moduleName = "Module";
        String expected = "Module required key1=\";\";";
        assertEquals(expected, AuthenticationUtils.JaasConfig(moduleName, options));
    }

    @Test
    public void testConfigWithEmptyOptions() {
        String moduleName = "ExampleModule";
        Map<String, String> options = new HashMap<>();

        String expectedOutput = "ExampleModule required ;";
        String result = AuthenticationUtils.JaasConfig(moduleName, options);

        assertEquals(expectedOutput, result);
    }

    //used for testing what keys and values does kafka accept.
    public static void main(String[] a) throws Exception {
        //org.apache.kafka.common.security.JaasConfig jaasConfig = new org.apache.kafka.common.security.JaasConfig();
        Class<?> aClass = Class.forName("org.apache.kafka.common.security.JaasConfig");

        Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(String.class, String.class);
        declaredConstructor.setAccessible(true);
        Configuration o = (Configuration) declaredConstructor.newInstance("Foo", "modulename required oauth.groups.claim.delimiter=\"value;1\";");
    }
}
