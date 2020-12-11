package io.strimzi.systemtest.annotations;

import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class SeqOrParallelTestCondition implements ExecutionCondition {

    private static final Logger LOGGER = LogManager.getLogger(SeqOrParallelTestCondition.class);

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        KubeClusterResource clusterResource = KubeClusterResource.getInstance();

        if (clusterResource.client().getClusterNodes().size() > 1) {

            // TODO: at runtime add @Execution(ExecutionMode.CONCURRENT) annotation

            Annotation testInstance = extensionContext.getTestInstance().get().getClass().getAnnotation(SeqOrParallelTest.class);

//            Method method = Class.class.getDeclaredMethod(extensionContext.getTestMethod().get().getName(), null);
//            method.setAccessible(true);
//
//            Map<String, Object> valuesMap = new HashMap<>();
//            valuesMap.put("@Execution", ExecutionMode.CONCURRENT);  // @Execution(ExecutionMode.CONCURRENT)
//
//            RuntimeAnnotations.putAnnotation(TracingST.class, Execution.class, valuesMap);
//
//            parallelTest = TestClass.class.getAnnotation(TestAnnotation.class);
//            System.out.println("TestClass annotation after:" + annotation);
            return ConditionEvaluationResult.enabled("Test will run in parallel");
        } else {
            // TODO: nothing to update...tests will run sequentially

            LOGGER.info("{} is @SeqOrParallelTest, but the running cluster is not multi-node cluster: Running test sequentially {}",
                extensionContext.getDisplayName(),
                extensionContext.getDisplayName()
            );
            return ConditionEvaluationResult.enabled("Test will run sequentially");
        }
    }

}
