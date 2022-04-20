package io.strimzi.systemtest.operators;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.StrimziPodSetTest;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;

@StrimziPodSetTest
public class PodSetIsolatedST extends AbstractST {

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
            .withNamespace(INFRA_NAMESPACE)
            .withExtraEnvVars()
            .createInstallation()
            .runInstallation();
    }
}
