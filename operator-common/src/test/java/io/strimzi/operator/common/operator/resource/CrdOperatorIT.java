/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.KafkaUserStatus;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class CrdOperatorIT<C extends KubernetesClient,
        T extends CustomResource,
        L extends CustomResourceList<T>,
        D extends Doneable<T>> extends AbstractResourceOperatorIT<C, T, L, D, Resource<T, D>> {

    protected final Logger log = LogManager.getLogger(getClass());
    CrdOperator<C, T, L, D> crdOperator;
    T customResource;

    @Override
    protected AbstractResourceOperator<C, T, L, D, Resource<T, D>> operator() {
        return new CrdOperator(vertx, client, KafkaUser.class, KafkaUserList.class, DoneableKafkaUser.class);
    }

    protected T getResource() {
        KafkaUser user = new KafkaUserBuilder().withApiVersion("kafka.strimzi.io/v1beta1").withMetadata(
                new ObjectMetaBuilder()
                        .withName(RESOURCE_NAME)
                        .withNamespace(namespace)
                        .build())
                .withNewSpec()
                .withNewKafkaUserTlsClientAuthentication()
                .endKafkaUserTlsClientAuthentication()
                .withNewKafkaUserAuthorizationSimple()
                .withAcls()
                .endKafkaUserAuthorizationSimple()
                .endSpec()
                .build();

        return (T) user;
    }

    @Override
    protected T getOriginal() {
        KafkaUser ku = (KafkaUser) getResource();
        log.info("{}", ku);
        log.info("================================================================");

        return (T) ku;
        //String kafkaUser = "test-user";

        //KafkaUser user = resources().User(CLUSTER_NAME, kafkaUser).done();
        //KUBE_CLIENT.waitForResourceCreation("kafkaUser", kafkaUser);
        //String kafkaUserAsJson = KUBE_CLIENT.getResourceAsJson("KafkaUser", kafkaUser);

        //customResource = crdOperator.get("myproject", "my-cluster");
        //crdOperator.updateStatus( (T) new KafkaBuilder((Kafka)customResource).withNewStatus().withState("").endStatus().build() );
        //customResource = crdOperator.get("myproject", "my-cluster");

        //log.info("Custom Resource: {}", customResource.toString() );
        //return customResource;
    }

    @Override
    protected T getModified()  {
        //crdOperator.updateStatus( (T) new KafkaBuilder((Kafka)customResource).withNewStatus().withState("foo").endStatus().build() );
        //T modifiedCustomResource = crdOperator.get("myproject", "my-cluster");

        //log.info("Updated Custom Resource: {}", modifiedCustomResource.toString() );
        //return modifiedCustomResource;
        //return customResource;

        KafkaUser ku = (KafkaUser)getResource();
        //KafkaUser ku = (KafkaUser) crdOperator.get(namespace, RESOURCE_NAME);
        KafkaUserStatus kus = new KafkaUserStatus();
        kus.setState("test");

        ku.setStatus(kus);

        log.info("{}", ku);

        return (T) ku;
    }

    @Override
    public void testFullCycle(TestContext context)    {
        Async async = context.async();
        AbstractResourceOperator op = operator();

        T newResource = getOriginal();
        T modResource = getModified();

        Future<ReconcileResult<T>> createFuture = op.reconcile(namespace, RESOURCE_NAME, newResource);

        createFuture.setHandler(create -> {
            if (create.succeeded()) {
                T created = (T) op.get(namespace, RESOURCE_NAME);

                if (created == null)    {
                    context.fail("Failed to get created Resource");
                    async.complete();
                } else  {

                    //crdOperator.updateStatus(modResource);

                    //assertResources(context, newResource, created);
                    async.complete();

                    /*Future<ReconcileResult<T>> modifyFuture = op.reconcile(namespace, RESOURCE_NAME, modResource);

                    modifyFuture.setHandler(modify -> {
                        if (modify.succeeded()) {
                            T modified = (T) op.get(namespace, RESOURCE_NAME);

                            if (modified == null)    {
                                context.fail("Failed to get modified Resource");
                                async.complete();
                            } else {
                                assertResources(context, modResource, modified);
                                // Override this
                                Future<ReconcileResult<T>> deleteFuture = op.reconcile(namespace, RESOURCE_NAME, null);

                                deleteFuture.setHandler(delete -> {
                                    if (delete.succeeded()) {
                                        T deleted = (T) op.get(namespace, RESOURCE_NAME);

                                        if (deleted == null)    {
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
                            context.fail(modify.cause());
                            async.complete();
                        }
                    });*/
                }

            } else {
                context.fail(create.cause());
                async.complete();
            }
        });
    }

    @Override
    protected void assertResources(TestContext context, T expected, T actual)   {
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
