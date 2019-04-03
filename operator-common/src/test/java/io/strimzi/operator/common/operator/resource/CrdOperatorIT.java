/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
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
        crdOperator = new CrdOperator(vertx, client, Kafka.class, KafkaList.class, DoneableKafka.class);
        return crdOperator;
    }

    @Override
    protected T getOriginal()  {
        customResource = crdOperator.get("myproject", "my-cluster");
        crdOperator.updateStatus( (T) new KafkaBuilder((Kafka)customResource).withNewStatus().withState("").endStatus().build() );
        customResource = crdOperator.get("myproject", "my-cluster");

        log.info("Custom Resource: {}", customResource.toString() );
        return customResource;
    }

    @Override
    protected T getModified()  {
        crdOperator.updateStatus( (T) new KafkaBuilder((Kafka)customResource).withNewStatus().withState("foo").endStatus().build() );
        T modifiedCustomResource = crdOperator.get("myproject", "my-cluster");
        log.info("================================================================");
        log.info("Updated Custom Resource: {}", modifiedCustomResource.toString() );
        return modifiedCustomResource;
    }

    //@Test
    //public void deleteStatus() {
    //    crdOperator.updateStatus( (T) new KafkaBuilder((Kafka)customResource).build() );
    //    T modifiedCustomResource = crdOperator.get("myproject", "my-cluster");
    //   log.info("================================================================");
    //    log.info("Updated Custom Resource: {}", modifiedCustomResource.toString() );
    //}

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
