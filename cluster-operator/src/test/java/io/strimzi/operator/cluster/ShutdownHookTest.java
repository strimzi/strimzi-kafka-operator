/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ShutdownHookTest {
    @Test
    public void testTerminationRunnable() {
        int nVerticles = 10;
        Vertx vertx = Vertx.vertx();
        List<MyVerticle> verticles = new ArrayList<>(nVerticles);
        while (verticles.size() < nVerticles) {
            MyVerticle myVerticle = new MyVerticle();
            vertx.deployVerticle(myVerticle);
            verticles.add(myVerticle);
        }
        assertThat("Verticles were not deployed", vertx.deploymentIDs(), hasSize(nVerticles));
        ShutdownHook hook = new ShutdownHook(vertx);
        hook.run();
        assertThat("Verticles were not closed", vertx.deploymentIDs(), empty());
        for (MyVerticle verticle : verticles) {
            assertThat("Verticle stop was not executed", verticle.getCounter(), is(1));
        }
    }
    
    static class MyVerticle extends AbstractVerticle {
        private int counter = 0;
        
        @Override
        public void stop() {
            counter++;
        }
        
        public int getCounter() {
            return counter;
        }
    } 
}
