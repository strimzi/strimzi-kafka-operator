package io.strimzi.operator.cluster.operator.assembly;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
public class JaasConfigTest {
    @Test
    public void testConfigWithValidInput() {
        String moduleName = "ExampleModule";
        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");
        options.put("option2", "value2");

        String expectedOutput = "ExampleModule required option1=\"value1\" option2=\"value2\";";
        String result = JaasConfig.config(moduleName, options);

        assertEquals(expectedOutput, result);
    }

    @Test
    public void testConfigWithNullModuleName() {
        String moduleName = null;
        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");

        assertThrows(NullPointerException.class, () -> {
            JaasConfig.config(moduleName, options);
        });
    }

    @Test
    public void testConfigWithNullOptionKey() {
        String moduleName = "ExampleModule";
        Map<String, String> options = new HashMap<>();
        options.put(null, "value1");

        assertThrows(NullPointerException.class, () -> {
            JaasConfig.config(moduleName, options);
        });
    }

    @Test
    public void testConfigWithNullOptionValue() {
        String moduleName = "ExampleModule";
        Map<String, String> options = new HashMap<>();
        options.put("option1", null);

        assertThrows(NullPointerException.class, () -> {
            JaasConfig.config(moduleName, options);
        });
    }

    @Test
    public void testConfigWithEmptyOptions() {
        String moduleName = "ExampleModule";
        Map<String, String> options = new HashMap<>();

        String expectedOutput = "ExampleModule required ;";
        String result = JaasConfig.config(moduleName, options);

        assertEquals(expectedOutput, result);
    }
}
