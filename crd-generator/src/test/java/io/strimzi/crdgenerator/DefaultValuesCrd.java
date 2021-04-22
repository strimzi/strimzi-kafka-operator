/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.crdgenerator.annotations.Crd;
import io.vertx.core.cli.annotations.DefaultValue;


// Accessed via reflection
@SuppressWarnings("unused")
@Crd(spec = @Crd.Spec(
        group = "crdgenerator.strimzi.io",
        names = @Crd.Spec.Names(
                kind = "Example",
                plural = "examples",
                categories = {"strimzi"}),
        scope = "Namespaced",
        versions = {
                @Crd.Spec.Version(name = "v1", served = true, storage = true),
                @Crd.Spec.Version(name = "v1beta2", served = true, storage = false)
        }))
public class DefaultValuesCrd extends CustomResource<Object, Object> {

    static final String DEFAULT_PRIMITIVE_SHORT = "10";
    static final String DEFAULT_BOXED_SHORT = "11";
    static final String DEFAULT_PRIMITIVE_INT = "100";
    static final String DEFAULT_BOXED_INT = "101";
    static final String DEFAULT_PRIMITIVE_LONG = "1000";
    static final String DEFAULT_BOXED_LONG = "1001";
    static final String DEFAULT_STRING = "default_string_value";
    static final String DEFAULT_CUSTOM_ENUM =  "two";
    static final String DEFAULT_NORMAL_ENUM = "BAR";

    @DefaultValue(DEFAULT_PRIMITIVE_SHORT)
    public short getDefaultPrimitiveShort() {
        return 1;
    }

    @DefaultValue(DEFAULT_BOXED_SHORT)
    public Short getDefaultBoxedShort() {
        return 1;
    }

    @DefaultValue(DEFAULT_PRIMITIVE_INT)
    public int getDefaultPrimitiveInt() {
        return 1;
    }

    @DefaultValue(DEFAULT_BOXED_INT)
    public Integer getDefaultBoxedInt() {
        return 1;
    }

    @DefaultValue(DEFAULT_PRIMITIVE_LONG)
    public long getDefaultPrimitiveLong() {
        return 1;
    }

    @DefaultValue(DEFAULT_BOXED_LONG)
    public Long getDefaultBoxedLong() {
        return 1L;
    }

    @DefaultValue(DEFAULT_STRING)
    public String getDefaultString() {
        return "Nop";
    }

    @DefaultValue(DEFAULT_CUSTOM_ENUM)
    public CustomisedEnum getDefaultCustomisedEnum() {
        return CustomisedEnum.ONE;
    }

    @DefaultValue(DEFAULT_NORMAL_ENUM)
    public NormalEnum getDefaultNormalEnum() {
        return NormalEnum.FOO;
    }
}
