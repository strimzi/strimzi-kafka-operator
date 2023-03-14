import com.yammer.metrics.core.Gauge;
import io.strimzi.kafka.agent.KafkaAgent;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaAgentTest {
    private Server server;
    private ServletContextHandler context;
    private final String testServerURL = "http://localhost:8080/";

    @Before
    public void setUp() {
        server = new Server(8080);
        context = new ServletContextHandler();
        ServletHolder defaultServ = new ServletHolder("default", DefaultServlet.class);
        context.addServlet(defaultServ,"/");
    }

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testBrokerRunningState() throws Exception {
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 3);
        KafkaAgent agent = new KafkaAgent(brokerState, null, null);
        context.setHandler(agent.getServerHandler());
        server.setHandler(context);
        server.start();

        HttpURLConnection http = (HttpURLConnection)new URL(testServerURL).openConnection();
        http.connect();
        assertEquals(http.getResponseCode(), (HttpStatus.OK_200));
        BufferedReader br = new BufferedReader(new InputStreamReader((http.getInputStream())));
        assertEquals("{\"brokerState\":3}", br.readLine());
    }

    @Test
    public void testBrokerRecoveryState() throws Exception {
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 2);

        final Gauge remainingLogs = mock(Gauge.class);
        when(remainingLogs.value()).thenReturn((byte) 10);

        final Gauge remainingSegments = mock(Gauge.class);
        when(remainingSegments.value()).thenReturn((byte) 100);

        KafkaAgent agent = new KafkaAgent(brokerState, remainingLogs, remainingSegments);
        context.setHandler(agent.getServerHandler());
        server.setHandler(context);
        server.start();

        HttpURLConnection http = (HttpURLConnection)new URL(testServerURL).openConnection();
        http.connect();
        assertEquals(http.getResponseCode(), (HttpStatus.OK_200));

        String expectedResponse = "{\"brokerState\":2,\"recoveryState\":{\"remainingLogsToRecover\":10,\"remainingSegmentsToRecover\":100}}";
        BufferedReader br = new BufferedReader(new InputStreamReader((http.getInputStream())));
        StringBuilder responseStrBuilder = new StringBuilder();
        String inputStr;
        while ((inputStr = br.readLine()) != null)
            responseStrBuilder.append(inputStr);
        assertEquals(expectedResponse, responseStrBuilder.toString());
    }

    @Test
    public void testBrokerMetricNotFound() throws Exception {
        KafkaAgent agent = new KafkaAgent(null, null, null);
        context.setHandler(agent.getServerHandler());
        server.setHandler(context);
        server.start();

        HttpURLConnection http = (HttpURLConnection)new URL(testServerURL).openConnection();
        http.connect();
        assertEquals(http.getResponseCode(), (HttpStatus.NOT_FOUND_404));
    }

}