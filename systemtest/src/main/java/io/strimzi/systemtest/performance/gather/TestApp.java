/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather;

import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.Level;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * Just application which should be deleted afterwards, just checks if metrics are correctly scraped and I'm saving some
 * time setup whole infrastructure;....
 * ---
 * Assumption is that infra is running i.e., Kafka cluster with UTO enabled...
 */
public class TestApp {

    private static TopicOperatorMetricsCollector topicOperatorMetricsCollector;
    private static TopicOperatorPollingThread topicOperatorPollingThread;
    private static Thread daemonThread;

    public static void main(String[] args) throws InterruptedException {
        kubeClient().listPods();

        String kafkaCr = cmdKubeClient().exec(true, Level.DEBUG, "get", "kafka", "-n", "co-namespace").out();

        // Regular expression pattern to match the Kafka cluster name
        Pattern pattern = Pattern.compile("^my-cluster-[a-f0-9]+", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(kafkaCr);

        String kafkaClusterName = "";

        // Check if the pattern matches and extract the cluster name
        if (matcher.find()) {
            kafkaClusterName = matcher.group();
            System.out.println(kafkaClusterName);
        } else {
            System.out.println("No Kafka cluster name found in the input string.");
        }

        final String allPods =  cmdKubeClient().exec(true, Level.DEBUG, "get", "pod", "-n", "co-namespace").out();

        // Regular expression pattern to match the scraper pod name
        pattern = Pattern.compile("^" + kafkaClusterName + "-scraper-[\\w-]+", Pattern.MULTILINE);
        matcher = pattern.matcher(allPods);

        String scraperPodName = "";

        // Check if the pattern matches and extract the scraper pod name
        if (matcher.find()) {
            scraperPodName = matcher.group();
            System.out.println(scraperPodName);
        } else {
            System.out.println("No scraper pod name found in the input string.");
        }

        // -- Metrics POLL --
        // Assuming 'testStorage' contains necessary details like namespace and scraperPodName
        topicOperatorMetricsCollector = new TopicOperatorMetricsCollector.Builder()
                .withScraperPodName(scraperPodName)
                .withNamespaceName("co-namespace")
                .withComponentType(ComponentType.TopicOperator)
                .withComponentName(kafkaClusterName)
                .build();

        topicOperatorPollingThread = new TopicOperatorPollingThread(topicOperatorMetricsCollector, "strimzi.io/cluster=" + kafkaClusterName);
        daemonThread = new Thread(topicOperatorPollingThread);
        daemonThread.setDaemon(true); // Set as daemon so it doesn't prevent JVM shutdown
        daemonThread.start();

        Thread.sleep(10_000);

        daemonThread.interrupt();
    }
}
