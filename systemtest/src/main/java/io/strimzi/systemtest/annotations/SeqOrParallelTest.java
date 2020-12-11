package io.strimzi.systemtest.annotations;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/***
 * Annotation for running parallel tests in strimzi test suite
 * please be sure that you know laws of parallel execution and concurrent programming
 * be sure that you do not use shared resources, and if you use shared resources please work with synchronization
 */
@Target(ElementType.METHOD)
@Retention(RUNTIME)
@Execution(ExecutionMode.CONCURRENT)
@Test
@ExtendWith(SeqOrParallelTestCondition.class)
public @interface SeqOrParallelTest {
}
