/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class CrdOperatorIT extends AbstractResourceOperatorIT<KubernetesClient, Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> {

    protected final Logger log = LogManager.getLogger(getClass());
    CrdOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka> crdOperator;
    Kafka customResource;

    @Override
    protected AbstractResourceOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> operator() {
        return new CrdOperator(vertx, client, Kafka.class, KafkaList.class, DoneableKafka.class);
    }

    protected Kafka getResource() {

        return new KafkaBuilder()
                .withApiVersion("kafka.strimzi.io/v1beta1")
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec().
                        withNewKafka()
                            .withReplicas(1)
                            .withNewListeners()
                                .withNewPlain()
                                .endPlain()
                            .endListeners()
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endKafka()
                        .withNewZookeeper()
                            .withReplicas(1)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endZookeeper()
                .endSpec()
                .withNewStatus()
                .endStatus().build();
    }

    @Override
    protected Kafka getOriginal() {
        return getResource();
    }

    @Override
    protected Kafka getModified()  {
        return new KafkaBuilder(getResource()).editSpec().editKafka().withReplicas(2).endKafka().endSpec().build();

    }

    /**
     * Needs CustomResourceOperationsImpl constructor patch for cascade deletion
     * https://github.com/fabric8io/kubernetes-client/pull/1325
     *
     * Remove this override when project is using fabric8 version > 4.1.1
     */
    @Override
    public void testFullCycle(TestContext context)    {
    }

    @Test
    public void statusTest(TestContext context) {
        Async async = context.async();

        crdOperator =  new CrdOperator(vertx, client, Kafka.class, KafkaList.class, DoneableKafka.class);

        Future<ReconcileResult<Kafka>> createFuture = crdOperator.reconcile(namespace, RESOURCE_NAME, getResource());

        createFuture.setHandler(create -> {
            if (create.succeeded()) {
                Kafka k0 = (Kafka) crdOperator.get(namespace, RESOURCE_NAME);

                if (k0 == null)    {
                    context.fail("Failed to get created Resource");
                    async.complete();
                } else {
                    // k1 needs the same resourceID as k0 in order to update it
                    Kafka k1 = new KafkaBuilder(k0).withNewStatus().withState("test").endStatus().build();
                    crdOperator.updateStatus(k1);
                    Kafka k2 = crdOperator.get(namespace, RESOURCE_NAME);
                    assertEquals(k1.getStatus().toString(), k2.getStatus().toString());
                    async.complete();

                    Future<ReconcileResult<Kafka>> deleteFuture = crdOperator.reconcile(namespace, RESOURCE_NAME, null);
                    deleteFuture.setHandler(delete -> {
                        if (delete.succeeded()) {
                            Kafka deleted = (Kafka) crdOperator.get(namespace, RESOURCE_NAME);

                            if (deleted == null)    {
                                log.info("Resource deleted");
                                async.complete();
                            } else {
                                context.fail("Failed to delete Resource");
                                async.complete();
                            }
                        } else {
                            context.fail(delete.cause());
                            async.complete();
                        }
                    });

                }

            } else {
                context.fail(create.cause());
                async.complete();
            }
        });
    }

    @Override
    protected void assertResources(TestContext context, Kafka expected, Kafka actual)   {
        context.assertEquals(expected.getMetadata().getName(), actual.getMetadata().getName());
        //context.assertEquals(expected.getMetadata().getNamespace(), actual.getMetadata().getNamespace());
        //context.assertEquals(expected.getMetadata().getLabels(), actual.getMetadata().getLabels());
        //context.assertEquals(expected.getSubjects().size(), actual.getSubjects().size());
        ///context.assertEquals(expected.getSubjects().get(0).getKind(), actual.getSubjects().get(0).getKind());
        //context.assertEquals(expected.getSubjects().get(0).getNamespace(), actual.getSubjects().get(0).getNamespace());
        //context.assertEquals(expected.getSubjects().get(0).getName(), actual.getSubjects().get(0).getName());

        //context.assertEquals(expected.getRoleRef().getKind(), actual.getRoleRef().getKind());
        //context.assertEquals(expected.getRoleRef().getApiGroup(), actual.getRoleRef().getApiGroup());
        //context.assertEquals(expected.getRoleRef().getName(), actual.getRoleRef().getName());

    }
}
