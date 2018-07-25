package io.strimzi.systemtest;

import io.strimzi.test.JUnitGroup;
import io.strimzi.test.KafkaFromClasspathYaml;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.strimzi.test.ClusterOperator;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StrimziRunner.class)
@Namespace(SecurityIT.NAMESPACE)
@ClusterOperator
public class SecurityIT extends AbstractIT {

    private static final Logger LOGGER = LogManager.getLogger(SecurityIT.class);
    public static final String NAMESPACE = "security-cluster-test";


    // oc adm policy --as system:admin add-cluster-role-to-user cluster-admin developer
    @Test
//    @JUnitGroup(name = "regression")
    @KafkaFromClasspathYaml()
    public void testTLS() {
        getZookeeperCertificateChain();
        kubeClient.execInPod("my-cluster-kafka-0", "/bin/bash", "-c", "openssl s_client -connect localhost:9093 -showcerts");

        String s = " 0 s:/O=io.strimzi/CN=my-cluster-kafka\n" +
                "   i:/O=io.strimzi/CN=cluster-ca";

        String s2 = " 1 s:/O=io.strimzi/CN=cluster-ca\n" +
                "   i:/O=io.strimzi/CN=cluster-ca";

        System.out.println();
    }

    private String getZookeeperCertificateChain(int podIndex){
        String kafkaPodName = kafkaPodName(CLUSTER_NAME, podIndex);
        return kubeClient.execInPod(kafkaPodName, "/bin/bash", "-c", "openssl s_client -connect localhost:9093 -showcerts").out();
    }


}
