/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.agent.server;

import com.yammer.metrics.core.Gauge;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class BrokerStateServlet extends HttpServlet {
    private final Gauge brokerState;
    public BrokerStateServlet(Gauge brokerState) {
        this.brokerState = brokerState;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/plain");
        response.setStatus(HttpServletResponse.SC_OK);
        if (brokerState != null) {
            response.getWriter().print(brokerState.value());
        } else {
            response.getWriter().print("broker state metric not found");
        }
    }
}
