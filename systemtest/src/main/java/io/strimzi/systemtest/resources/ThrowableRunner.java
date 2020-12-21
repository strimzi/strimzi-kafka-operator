package io.strimzi.systemtest.resources;

@FunctionalInterface
public interface ThrowableRunner {
    void run() throws Exception;
}
