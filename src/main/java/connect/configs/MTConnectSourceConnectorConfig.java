package connect.configs;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MTConnectSourceConnectorConfig extends AbstractConfig {

    // properties
    public static final String TOPIC = "topic";
    public static final String TOPIC_DOC = "Topic to write data";

    public static final String UNREACHABLE_AGENTS_TOPIC = "unreachable.agents.topic";
    private static final String UNREACHABLE_AGENTS_TOPIC_DOC = "If not successfully connected to the agent or " +
            "connection fails while getting data, AGENT_URL will be sent to the specified Kafka topic with " +
            "the error message and request timestamp.";
    private static final String UNREACHABLE_AGENTS_TOPIC_DISPLAY_NAME = "Kafka Topic for Unreachable Agent URLs";

    public static final String ALWAYS_FROM_LATEST = "always.from.latest";
    private static final String ALWAYS_FROM_LATEST_DOC = "If this property is true, each time you connect " +
            "to the MTConnect agent, data will be received from the ending of the MTConnect agent buffer. " +
            "If you want to minimize the data loss, set this property to false. If it's false, " +
            "task firstly will try to use saved offsets. If any offsets not found, it'll receive data " +
            "from the beginning of the buffer. If there is found any offsets, it checks the current buffer " +
            "status of the agent. If the agent did not restarted, it'll check whether data with saved offsets already " +
            "exist in the buffer. If data exist, it'll try to get data using saved offsets. If not, it'll be " +
            "get data from the beginning of the buffer.";
    private static final String ALWAYS_FROM_LATEST_DISPLAY_NAME = "Always Get From the Latest";

    public static final String AGENT_URLS = "agent.urls";
    private static final String AGENT_URLS_DOC = "URLs that receive data.";
    private static final String AGENT_URLS_DISPLAY_NAME = "Agent URLs";

    public static final String INTERVAL = "mtconnect.interval.ms";
    private static final String INTERVAL_DOC = "Send data every specified time if there is new data" +
            " in the buffer. Must be millisecond.";
    private static final String INTERVAL_DISPLAY_NAME = "MTConnect Interval";

    public static final String COUNT = "mtconnect.count";
    private static final String COUNT_DOC = "Maximum number of data to be sent each time.";
    private static final String COUNT_DISPLAY_NAME = "MTConnect Count";

    public static final String HEARTBEAT = "mtconnect.heartbeat.ms";
    private static final String HEARTBEAT_DOC = "Interval to prevent connection fails between client and MTConnect " +
            "Agent. If there is no data available in the buffer, the agent must send out a heartbeat to " +
            "maintain contact.";
    private static final String HEARTBEAT_DISPLAY_NAME = "MTConnect Heartbeat";

    public static final String CONNECT_TIMEOUT = "http.connect.timeout.ms";
    private static final String CONNECT_TIMEOUT_DOC = "Maximum time to connect to the MTConnect Agent.";
    private static final String CONNECT_TIMEOUT_DISPLAY_NAME = "HTTP Connect Timeout";

    public static final String READ_TIMEOUT = "http.read.timeout.ms";
    private static final String READ_TIMEOUT_DOC = "Maximum time to read data from the MTConnect Agent. Value of " +
            "this property must be greater than the 'heartbeat' value. If not, the connection will be closed each time " +
            "if there is no available new data in the agent.";
    private static final String READ_TIMEOUT_DISPLAY_NAME = "HTTP Read Timeout";

    // groups
    public static final String GROUP_GENERAL = "general";
    public static final String GROUP_MTCONNECT = "mtconnect";
    public static final String GROUP_HTTP = "http";

    public MTConnectSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }

    public MTConnectSourceConnectorConfig(ConfigDef subConf, Map<String, String> props) {
        super(subConf, props);
    }

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public static ConfigDef baseConfigDef() {
        ConfigDef config = new ConfigDef();
        addGeneralOptions(config);
        addMTConnectOptions(config);
        addHttpOptions(config);
        return config;
    }

    private static final void addGeneralOptions(ConfigDef config) {
        int orderInGroup = 0;
        config
                .define(TOPIC,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        TOPIC_DOC)
                .define(UNREACHABLE_AGENTS_TOPIC,
                        ConfigDef.Type.STRING,
                        "unreachable-mtconnect-agents",
                        ConfigDef.Importance.HIGH,
                        UNREACHABLE_AGENTS_TOPIC_DOC,
                        GROUP_GENERAL,
                        ++orderInGroup,
                        ConfigDef.Width.MEDIUM,
                        UNREACHABLE_AGENTS_TOPIC_DISPLAY_NAME)
                .define(ALWAYS_FROM_LATEST,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.HIGH,
                        ALWAYS_FROM_LATEST_DOC,
                        GROUP_GENERAL,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        ALWAYS_FROM_LATEST_DISPLAY_NAME)
                .define(AGENT_URLS,
                        ConfigDef.Type.LIST,
                        null,
                        ConfigDef.Importance.HIGH,
                        AGENT_URLS_DOC,
                        GROUP_GENERAL,
                        ++orderInGroup,
                        ConfigDef.Width.LONG,
                        AGENT_URLS_DISPLAY_NAME);
    }

    private static final void addMTConnectOptions(ConfigDef config) {
        int orderInGroup = 0;
        config
                .define(INTERVAL,
                        ConfigDef.Type.INT,
                        1000,
                        ConfigDef.Importance.HIGH,
                        INTERVAL_DOC,
                        GROUP_MTCONNECT,
                        ++orderInGroup,
                        ConfigDef.Width.MEDIUM,
                        INTERVAL_DISPLAY_NAME)
                .define(COUNT,
                        ConfigDef.Type.INT,
                        100,
                        ConfigDef.Importance.HIGH,
                        COUNT_DOC,
                        GROUP_MTCONNECT,
                        ++orderInGroup,
                        ConfigDef.Width.MEDIUM,
                        COUNT_DISPLAY_NAME)
                .define(HEARTBEAT,
                        ConfigDef.Type.INT,
                        5000,
                        ConfigDef.Importance.HIGH,
                        HEARTBEAT_DOC,
                        GROUP_MTCONNECT,
                        ++orderInGroup,
                        ConfigDef.Width.MEDIUM,
                        HEARTBEAT_DISPLAY_NAME);

    }

    private static final void addHttpOptions(ConfigDef config) {
        int orderInGroup = 0;
        config
                .define(CONNECT_TIMEOUT,
                        ConfigDef.Type.INT,
                        15000,
                        ConfigDef.Importance.LOW,
                        CONNECT_TIMEOUT_DOC,
                        GROUP_HTTP,
                        ++orderInGroup,
                        ConfigDef.Width.MEDIUM,
                        CONNECT_TIMEOUT_DISPLAY_NAME)
                .define(READ_TIMEOUT,
                        ConfigDef.Type.INT,
                        15000,
                        ConfigDef.Importance.LOW,
                        READ_TIMEOUT_DOC,
                        GROUP_HTTP,
                        ++orderInGroup,
                        ConfigDef.Width.MEDIUM,
                        READ_TIMEOUT_DISPLAY_NAME);
    }
}
