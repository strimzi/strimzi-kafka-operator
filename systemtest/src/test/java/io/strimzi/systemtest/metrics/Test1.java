package io.strimzi.systemtest.metrics;

import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.test.interfaces.ExtensionContextParameterResolver;
import kafka.log.Log;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Random;

@ExtendWith(ExtensionContextParameterResolver.class)
public class Test1 {

    @ParallelTest
    void test123(ExtensionContext extensionContext) {
        System.out.println("here");
        extensionContext.publishReportEntry("NAMESPACE");

        System.out.println(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get("NAMESPACE").toString());
    }

    @ParallelTest
    void test1234(ExtensionContext extensionContext) {
        System.out.println("here");
        extensionContext.publishReportEntry("NAMESPACE");

        System.out.println(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get("NAMESPACE").toString());
    }

    @ParallelTest
    void test1235(ExtensionContext extensionContext) {
        System.out.println("here");
        extensionContext.publishReportEntry("NAMESPACE");

        System.out.println(extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get("NAMESPACE").toString());
    }

    @BeforeEach
    void before(ExtensionContext extensionContext) {
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put("NAMESPACE", "hello-namespace" + new Random().nextInt(Integer.MAX_VALUE));

        extensionContext.publishReportEntry("NAMESPACE", "hello-namespace");
    }

    @BeforeAll
    void beforeAll(ExtensionContext extensionContext) {
        System.out.println("Here before all");
    }
}
