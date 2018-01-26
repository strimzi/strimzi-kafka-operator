package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.vertx.core.*;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CreateKafkaClusterOperation extends SimpleClusterOperation<KafkaCluster> {
    private static final Logger log = LoggerFactory.getLogger(CreateKafkaClusterOperation.class.getName());

    public CreateKafkaClusterOperation(String namespace, String name) {
        super("kafka", "create", namespace, name);
    }

    @Override
    protected KafkaCluster getCluster(K8SUtils k8s, Handler handler, Lock lock) {
        return KafkaCluster.fromConfigMap(k8s.getConfigmap(namespace, name));
    }

    @Override
    protected List<Future> creationFutures(K8SUtils k8s, KafkaCluster kafka) {
        List<Future> result = new ArrayList<>(4);
        // start creating configMap operation only if metrics are enabled,
        // otherwise the future is already complete (for the "join")
        if (kafka.isMetricsEnabled()) {
            Future<Void> futureConfigMap = Future.future();
            OperationExecutor.getInstance().executeK8s(CreateOperation.createConfigMap(kafka.generateMetricsConfigMap()), futureConfigMap.completer());
            result.add(futureConfigMap);
        }

        Future<Void> futureService = Future.future();
        OperationExecutor.getInstance().executeFabric8(CreateOperation.createService(kafka.generateService()), futureService.completer());
        result.add(futureService);

        Future<Void> futureHeadlessService = Future.future();
        OperationExecutor.getInstance().executeFabric8(CreateOperation.createService(kafka.generateHeadlessService()), futureHeadlessService.completer());
        result.add(futureHeadlessService);

        Future<Void> futureStatefulSet = Future.future();
        OperationExecutor.getInstance().executeK8s(CreateOperation.createStatefulSet(kafka.generateStatefulSet(k8s.isOpenShift())), futureStatefulSet.completer());
        result.add(futureStatefulSet);

        return result;
    }
}
