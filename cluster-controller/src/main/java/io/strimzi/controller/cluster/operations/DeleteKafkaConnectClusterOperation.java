package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.openshift.DeleteS2IOperation;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.vertx.core.*;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DeleteKafkaConnectClusterOperation extends SimpleClusterOperation<KafkaConnectCluster> {
    private static final Logger log = LoggerFactory.getLogger(DeleteKafkaConnectClusterOperation.class.getName());

    public DeleteKafkaConnectClusterOperation(Vertx vertx, String namespace, String name) {
        super(vertx, "kafka-connect", "delete", namespace, name);
    }

    @Override
    protected List<Future> futures(K8SUtils k8s, KafkaConnectCluster connect) {
        List<Future> result = new ArrayList<>(3);

        Future<Void> futureService = Future.future();
        OperationExecutor.getInstance().executeFabric8(DeleteOperation.deleteService(namespace, connect.getName()), futureService.completer());
        result.add(futureService);

        Future<Void> futureDeployment = Future.future();
        OperationExecutor.getInstance().executeFabric8(DeleteOperation.deleteDeployment(namespace, connect.getName()), futureDeployment.completer());
        result.add(futureDeployment);

        if (connect.getS2I() != null) {
            Future<Void> futureS2I = Future.future();
            OperationExecutor.getInstance().executeOpenShift(new DeleteS2IOperation(connect.getS2I()), futureS2I.completer());
            result.add(futureS2I);
        }

        return result;
    }

    @Override
    protected KafkaConnectCluster getCluster(K8SUtils k8s, Handler<AsyncResult<Void>> handler, Lock lock) {
        return KafkaConnectCluster.fromDeployment(k8s, namespace, name);
    }
}
