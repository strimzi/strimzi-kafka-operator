/*
 * Copyright 2019, Strimzi authors.
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
    private String name;
    private Scope scope;
    private Type type;
    private Number minimum;
    private Number maximum;
    private List<String> items;
    @JsonProperty("enum")
    private List<String> values;
    private String pattern;

    /**
     * @return The name of the parameter.
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return The scope of the parameter.
     */
    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    /**
     * @return Inclusive minimum for a parameter of numeric type.
     */
    public Number getMinimum() {
        return minimum;
    }

    public void setMinimum(Number minimum) {
        this.minimum = minimum;
    }

    /**
     * @return Inclusive maximum for a parameter of numeric type.
     */
    public Number getMaximum() {
        return maximum;
    }

    public void setMaximum(Number maximum) {
        this.maximum = maximum;
    }

    /**
     * @return The allowed items for the parameter value, for parameters of list type.
     */
    public List<String> getItems() {
        return items;
    }

    public void setItems(List<String> items) {
        this.items = items;
    }

    /**
     * @return The allowed values for the parameter value, for parameters of string type.
     */
    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    /**
     * @return A regular expression which values must match, for parameters of string type.
     */
    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public List<String> validate(String value) {
        switch (getType()) {
            case BOOLEAN:
                return validateBoolean(value);
            case STRING:
                return validateString(value);
            case INT:
                return validateInt(value);
            case LONG:
                return validateLong(value);
            case DOUBLE:
                return validateDouble(value);
            case SHORT:
                return validateShort(value);
            case CLASS:
                return emptyList();
            case PASSWORD:
                return emptyList();
            case LIST:
                return validateList(value);
            default:
                throw new IllegalStateException("Unsupported type " + getType());
        }
    }

    private List<String> validateString(String value) {
        List<String> errors = emptyList();
        if (getValues() != null
                && !getValues().contains(value)) {
            errors = new ArrayList<>(1);
            errors.add(getName() + " has value '" + value + "' which is not one of the allowed values: " + getValues());
        }
        if (getPattern() != null
                && !value.matches(getPattern())) {
            if (errors.isEmpty()) {
                errors = new ArrayList<>(1);
            }
            errors.add(getName() + " has value '" + value + "' which does not match the required pattern: " + getPattern());
        }
        return errors;
    }

    private List<String> validateBoolean(String value) {
        if (!value.matches("true|false")) {
            return singletonList(getName() + " has value '" + value + "' which is not a boolean");
        }
        return emptyList();
    }

    private List<String> validateList(String value) {
        List<String> l = asList(value.trim().split(" *, *"));
        if (getItems() != null) {
            HashSet<String> items = new HashSet<>(l);
            items.removeAll(getItems());
            if (!items.isEmpty()) {
                return singletonList(getName() + " contains values " + items + " which are not in the allowed items " + getItems());
            }
        }
        return emptyList();
    }

    private List<String> validateDouble(String value) {
        List<String> errors = emptyList();
        try {
            double i = Double.parseDouble(value);
            if (getMinimum() != null
                    && i < getMinimum().doubleValue()) {
                errors = new ArrayList<>(1);
                errors.add(minimumErrorMsg(value));
            }
            if (getMaximum() != null
                    && i > getMaximum().doubleValue()) {
                if (errors.isEmpty()) {
                    errors = new ArrayList<>(1);
                }
                errors.add(maximumErrorMsg(value));
            }
        } catch (NumberFormatException e) {
            errors = singletonList(numFormatMsg(value, "a double"));
        }
        return errors;
    }

    private List<String> validateLong(String value) {
        List<String> errors = emptyList();
        try {
            long i = Long.parseLong(value);
            if (getMinimum() != null
                    && i < getMinimum().longValue()) {
                errors = new ArrayList<>(1);
                errors.add(minimumErrorMsg(value));
            }
            if (getMaximum() != null
                    && i > getMaximum().longValue()) {
                if (errors.isEmpty()) {
                    errors = new ArrayList<>(1);
                }
                errors.add(maximumErrorMsg(value));
            }
        } catch (NumberFormatException e) {
            errors = singletonList(numFormatMsg(value, "a long"));
        }
        return errors;
    }

    private List<String> validateShort(String value) {
        List<String> errors = emptyList();
        try {
            short i = Short.parseShort(value);
            if (getMinimum() != null
                    && i < getMinimum().shortValue()) {
                errors = new ArrayList<>(1);
                errors.add(minimumErrorMsg(value));
            }
            if (getMaximum() != null
                    && i > getMaximum().shortValue()) {
                if (errors.isEmpty()) {
                    errors = new ArrayList<>(1);
                }
                errors.add(maximumErrorMsg(value));
            }
        } catch (NumberFormatException e) {
            errors = singletonList(numFormatMsg(value, "a short"));
        }
        return errors;
    }

    private List<String> validateInt(String value) {
        List<String> errors = emptyList();
        try {
            int i = Integer.parseInt(value);
            if (getMinimum() != null
                    && i < getMinimum().intValue()) {
                errors = new ArrayList<>(1);
                errors.add(minimumErrorMsg(value));
            }
            if (getMaximum() != null
                    && i > getMaximum().intValue()) {
                if (errors.isEmpty()) {
                    errors = new ArrayList<>(1);
                }
                errors.add(maximumErrorMsg(value));
            }
        } catch (NumberFormatException e) {
            errors = singletonList(numFormatMsg(value, "an int"));
        }
        return errors;
    }

    private String minimumErrorMsg(String value) {
        return getName() + " has value " + value + " which less than the minimum value " + getMinimum();
    }

    private String maximumErrorMsg(String value) {
        return getName() + " has value " + value + " which greater than the maximum value " + getMaximum();
    }

    private String numFormatMsg(String value, String typeDescription) {
        return getName() + " has value '" + value + "' which is not " + typeDescription;
    }
}
