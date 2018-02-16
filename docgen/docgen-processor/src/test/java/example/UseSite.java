/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package example;

//@Table(title="", rows=EnvVar.class, headers=true)
class UseSite {
    @EnvVar(name="FOO", doc="Documentation about FOO", defaultValue = "null")
    public static final String ENV_VAR_FOO = "FOO";

    @EnvVar(name="BAR", doc="Documentation about BAR", defaultValue = "bar", required = true)
    public static final String ENV_VAR_BAR = "BAR";
}
