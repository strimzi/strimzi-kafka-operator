/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.logs;

import java.util.Objects;

public class CollectorElement {

    private String testClassName;
    private String testMethodName;

    public CollectorElement(String testClass, String testTest) {
        this.testClassName = testClass;
        this.testMethodName = testTest;
    }

    public CollectorElement(String testClass) {
        this.testClassName = testClass;
        this.testMethodName = "";
    }

    public static CollectorElement emptyElement() {
        return new CollectorElement("", "");
    }

    public static CollectorElement createCollectorElement(String testClass) {
        return new CollectorElement(testClass);
    }

    public static CollectorElement createCollectorElement(String testClass, String testMethod) {
        return new CollectorElement(testClass, testMethod);
    }

    public String getTestClassName() {
        return testClassName;
    }

    public String getTestMethodName() {
        return testMethodName;
    }

    public void setTestMethodName(String testMethodName) {
        this.testMethodName = testMethodName;
    }

    public boolean isEmpty() {
        return this.testClassName.equals("") && this.testMethodName.equals("");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CollectorElement that = (CollectorElement) o;
        return Objects.equals(testClassName, that.testClassName) && Objects.equals(testMethodName, that.testMethodName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(testClassName, testMethodName);
    }
}
