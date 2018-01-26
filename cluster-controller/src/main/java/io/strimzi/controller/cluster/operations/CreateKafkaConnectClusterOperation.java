package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.openshift.CreateS2IOperation;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.vertx.core.*;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CreateKafkaConnectClusterOperation extends CreateClusterOperation<KafkaConnectCluster> {
    private static final Logger log = LoggerFactory.getLogger(CreateKafkaConnectClusterOperation.class.getName());

    public CreateKafkaConnectClusterOperation(String namespace, String name) {
        super("kafka-connect", namespace, name);
    }

    @Override
    protected KafkaConnectCluster getCluster(K8SUtils k8s, Handler<AsyncResult<Void>> handler, Lock lock) {
        return KafkaConnectCluster.fromConfigMap(k8s, k8s.getConfigmap(namespace, name));
    }

    @Override
    protected List<Future> creationFutures(K8SUtils k8s, KafkaConnectCluster connect) {
        List<Future> result = new ArrayList<>(4);
        Future<Void> futureService = Future.future();
        OperationExecutor.getInstance().executeFabric8(CreateOperation.createService(connect.generateService()), futureService.completer());
        result.add(futureService);

        Future<Void> futureDeployment = Future.future();
        OperationExecutor.getInstance().executeK8s(CreateOperation.createDeployment(connect.generateDeployment()), futureDeployment.completer());
        result.add(futureDeployment);

        Future<Void> futureS2I;
        if (connect.getS2I() != null) {
            futureS2I = Future.future();
            OperationExecutor.getInstance().executeOpenShift(new CreateS2IOperation(connect.getS2I()), futureS2I.completer());
        } else {
            futureS2I = Future.succeededFuture();
        }
        result.add(futureS2I);

        return result;
    }

}
