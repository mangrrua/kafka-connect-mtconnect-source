package connect;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static connect.configs.MTConnectSourceConnectorConfig.*;
import static org.junit.Assert.assertEquals;

public class MTConnectConnectorTest {

    private MTConnectSourceConnector connector = new MTConnectSourceConnector();

    private Map<String, String> initializeConfigs() {
        Map<String, String> configs = new HashMap<>();
        configs.put(TOPIC, "test-topic");
        configs.put(UNREACHABLE_AGENTS_TOPIC, "unreachable-topic");
        configs.put(ALWAYS_FROM_LATEST, "true");
        configs.put(AGENT_URLS, "http://mtconnect.mazakcorp.com:5609,http://mtconnect.mazakcorp.com:5610,http://mtconnect.mazakcorp.com:5611,http://mtconnect.mazakcorp.com:5612");
        configs.put(INTERVAL, "1000");
        configs.put(COUNT, "100");
        configs.put(HEARTBEAT, "5000");
        configs.put(CONNECT_TIMEOUT, "15000");
        configs.put(READ_TIMEOUT, "15000");
        return configs;
    }

    @Test
    public void taskConfigsTest() {
        connector.start(initializeConfigs());
        List<Map<String, String>> configs = connector.taskConfigs(4);
        assertEquals(configs.size(), 4);
    }
}
