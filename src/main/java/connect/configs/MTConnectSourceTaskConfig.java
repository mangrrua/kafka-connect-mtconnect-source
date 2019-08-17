package connect.configs;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MTConnectSourceTaskConfig extends MTConnectSourceConnectorConfig {

    public static final String AGENT_URL = "agent.url";
    public static final String AGENT_URL_DOC = "Agent URL which will be used by task to get data.";

    static ConfigDef config = baseConfigDef()
            .define(AGENT_URL,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    AGENT_URL_DOC);

    public MTConnectSourceTaskConfig(Map<String, String> props) {
        super(config, props);
    }

    public String getTopic() {
        return this.getString(TOPIC);
    }

    public String getUnreachableTopic() {
        return this.getString(UNREACHABLE_AGENTS_TOPIC);
    }

    public String getUrl() {
        return this.getString(AGENT_URL);
    }

    public boolean isFromLatest() {
        return this.getBoolean(ALWAYS_FROM_LATEST);
    }

    public int getInterval() {
        return this.getInt(INTERVAL);
    }

    public int getCount() {
        return this.getInt(COUNT);
    }

    public int getHeartbeat() {
        return this.getInt(HEARTBEAT);
    }

    public int getConnectTimeout() {
        return this.getInt(CONNECT_TIMEOUT);
    }

    public int getReadTimeout() {
        return this.getInt(READ_TIMEOUT);
    }
}
