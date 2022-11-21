/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.config.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * A model of a particular configuration parameter.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConfigModel {
    private Scope scope;
    private Type type;
    private Number minimum;
    private Number maximum;
    private List<String> items;
    @JsonProperty("enum")
    private List<String> values;
    private String pattern;

    /**
     * @return The scope of the parameter.
     */
    public Scope getScope() {
        return scope;
    }

    /**
     * Sets the scope of the parameter
     *
     * @param scope Scope of the parameter
     */
    public void setScope(Scope scope) {
        this.scope = scope;
    }

    /**
     * @return  Type of the parameter
     */
    public Type getType() {
        return type;
    }

    /**
     * Sets the type of the parameter
     *
     * @param type  Type of the parameter
     */
    public void setType(Type type) {
        this.type = type;
    }

    /**
     * @return Inclusive minimum for a parameter of numeric type.
     */
    public Number getMinimum() {
        return minimum;
    }

    /**
     * Sets the minimum value of the parameter
     *
     * @param minimum   Minimum value of the parameter
     */
    public void setMinimum(Number minimum) {
        this.minimum = minimum;
    }

    /**
     * @return Inclusive maximum for a parameter of numeric type.
     */
    public Number getMaximum() {
        return maximum;
    }

    /**
     * Sets the maximum value of the parameter
     *
     * @param maximum   Maximum value of the parameter
     */
    public void setMaximum(Number maximum) {
        this.maximum = maximum;
    }

    /**
     * @return The allowed items for the parameter value, for parameters of list type.
     */
    public List<String> getItems() {
        return items;
    }

    /**
     * Sets the allowed items for a list parameter
     *
     * @param items Allowed items
     */
    public void setItems(List<String> items) {
        this.items = items;
    }

    /**
     * @return The allowed values for the parameter value, for parameters of string type.
     */
    public List<String> getValues() {
        return values;
    }

    /**
     * Sets the allowed values of String parameter
     *
     * @param values    Allowed values
     */
    public void setValues(List<String> values) {
        this.values = values;
    }

    /**
     * @return A regular expression which values must match, for parameters of string type.
     */
    public String getPattern() {
        return pattern;
    }

    /**
     * Sets the pattern of the parameter
     *
     * @param pattern   Pattern
     */
    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    /**
     * Validates the option
     *
     * @param configName    Name of the option
     * @param value         Value of the option
     *
     * @return  List of errors found during the validation. Empty if no errors were found.
     */
    public List<String> validate(String configName, String value) {
        switch (getType()) {
            case BOOLEAN:
                return validateBoolean(configName, value);
            case STRING:
                return validateString(configName, value);
            case INT:
                return validateInt(configName, value);
            case LONG:
                return validateLong(configName, value);
            case DOUBLE:
                return validateDouble(configName, value);
            case SHORT:
                return validateShort(configName, value);
            case CLASS:
                return emptyList();
            case PASSWORD:
                return emptyList();
            case LIST:
                return validateList(configName, value);
            default:
                throw new IllegalStateException("Unsupported type " + getType());
        }
    }

    private List<String> validateString(String configName, String value) {
        List<String> errors = emptyList();
        if (getValues() != null
                && !getValues().contains(value)) {
            errors = new ArrayList<>(1);
            errors.add(configName + " has value '" + value + "' which is not one of the allowed values: " + getValues());
        }
        if (getPattern() != null
                && !value.matches(getPattern())) {
            if (errors.isEmpty()) {
                errors = new ArrayList<>(1);
            }
            errors.add(configName + " has value '" + value + "' which does not match the required pattern: " + getPattern());
        }
        return errors;
    }

    private List<String> validateBoolean(String configName, String value) {
        if (!value.matches("true|false")) {
            return singletonList(configName + " has value '" + value + "' which is not a boolean");
        }
        return emptyList();
    }

    private List<String> validateList(String configName, String value) {
        List<String> l = asList(value.trim().split(" *, *", -1));
        if (getItems() != null) {
            HashSet<String> items = new HashSet<>(l);
            items.removeAll(getItems());
            if (!items.isEmpty()) {
                return singletonList(configName + " contains values " + items + " which are not in the allowed items " + getItems());
            }
        }
        return emptyList();
    }

    private List<String> validateDouble(String configName, String value) {
        List<String> errors = emptyList();
        try {
            double i = Double.parseDouble(value);
            if (getMinimum() != null
                    && i < getMinimum().doubleValue()) {
                errors = new ArrayList<>(1);
                errors.add(minimumErrorMsg(configName, value));
            }
            if (getMaximum() != null
                    && i > getMaximum().doubleValue()) {
                if (errors.isEmpty()) {
                    errors = new ArrayList<>(1);
                }
                errors.add(maximumErrorMsg(configName, value));
            }
        } catch (NumberFormatException e) {
            errors = singletonList(numFormatMsg(configName, value, "a double"));
        }
        return errors;
    }

    private List<String> validateLong(String configName, String value) {
        List<String> errors = emptyList();
        try {
            long i = Long.parseLong(value);
            if (getMinimum() != null
                    && i < getMinimum().longValue()) {
                errors = new ArrayList<>(1);
                errors.add(minimumErrorMsg(configName, value));
            }
            if (getMaximum() != null
                    && i > getMaximum().longValue()) {
                if (errors.isEmpty()) {
                    errors = new ArrayList<>(1);
                }
                errors.add(maximumErrorMsg(configName, value));
            }
        } catch (NumberFormatException e) {
            errors = singletonList(numFormatMsg(configName, value, "a long"));
        }
        return errors;
    }

    private List<String> validateShort(String configName, String value) {
        List<String> errors = emptyList();
        try {
            short i = Short.parseShort(value);
            if (getMinimum() != null
                    && i < getMinimum().shortValue()) {
                errors = new ArrayList<>(1);
                errors.add(minimumErrorMsg(configName, value));
            }
            if (getMaximum() != null
                    && i > getMaximum().shortValue()) {
                if (errors.isEmpty()) {
                    errors = new ArrayList<>(1);
                }
                errors.add(maximumErrorMsg(configName, value));
            }
        } catch (NumberFormatException e) {
            errors = singletonList(numFormatMsg(configName, value, "a short"));
        }
        return errors;
    }

    private List<String> validateInt(String configName, String value) {
        List<String> errors = emptyList();
        try {
            int i = Integer.parseInt(value);
            if (getMinimum() != null
                    && i < getMinimum().intValue()) {
                errors = new ArrayList<>(1);
                errors.add(minimumErrorMsg(configName, value));
            }
            if (getMaximum() != null
                    && i > getMaximum().intValue()) {
                if (errors.isEmpty()) {
                    errors = new ArrayList<>(1);
                }
                errors.add(maximumErrorMsg(configName, value));
            }
        } catch (NumberFormatException e) {
            errors = singletonList(numFormatMsg(configName, value, "an int"));
        }
        return errors;
    }

    private String minimumErrorMsg(String configName, String value) {
        return configName + " has value " + value + " which less than the minimum value " + getMinimum();
    }

    private String maximumErrorMsg(String configName, String value) {
        return configName + " has value " + value + " which greater than the maximum value " + getMaximum();
    }

    private String numFormatMsg(String configName, String value, String typeDescription) {
        return configName + " has value '" + value + "' which is not " + typeDescription;
    }
}
