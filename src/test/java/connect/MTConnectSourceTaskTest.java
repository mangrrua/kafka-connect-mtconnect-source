package connect;

import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;
import org.junit.Test;

import java.util.*;

import static connect.configs.MTConnectSourceConnectorConfig.*;
import static connect.configs.MTConnectSourceTaskConfig.AGENT_URL;
import static org.junit.Assert.*;

public class MTConnectSourceTaskTest {

    private MTConnectSourceTask task = new MTConnectSourceTask();

    private Map<String, String> initialTaskConfigs() {
        Map<String, String> configs = new HashMap<>();
        configs.put(AGENT_URL, "http://mtconnect.mazakcorp.com:5611");
        configs.put(TOPIC, "test");
        configs.put(UNREACHABLE_AGENTS_TOPIC, "unreachable-agents-topic");
        configs.put(ALWAYS_FROM_LATEST, "true");
        configs.put(INTERVAL, "1000");
        configs.put(COUNT, "100");
        configs.put(HEARTBEAT, "5000");
        configs.put(CONNECT_TIMEOUT, "15000");
        configs.put(READ_TIMEOUT, "15000");
        return configs;
    }

    @Test
    public void buildUnreachableAgentRecordTest() {
        task.start(initialTaskConfigs());

        String errorMessage = "an error message";
        SourceRecord record = task.generateUnreachableAgentRecord(errorMessage);
        assertEquals(record.topic(), "unreachable-agents-topic");

        String value = (String) record.value();
        JSONObject jsonValue = new JSONObject(value);

        assertEquals(jsonValue.get("agent_url"), "http://mtconnect.mazakcorp.com:5611");
        assertEquals(jsonValue.get("error_message"), errorMessage);
    }
}
