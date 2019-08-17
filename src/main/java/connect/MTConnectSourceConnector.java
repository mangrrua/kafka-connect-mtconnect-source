package connect;

import connect.configs.MTConnectSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static connect.configs.MTConnectSourceConnectorConfig.*;
import static connect.configs.MTConnectSourceTaskConfig.AGENT_URL;

public class MTConnectSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(MTConnectSourceConnector.class);

    private Map<String, String> configProperties;
    private MTConnectSourceConnectorConfig config;

    @Override
    public void start(Map<String, String> props) {
        log.info("Connector is starting with props = {}", props);

        try {
            configProperties = props;
            config = new MTConnectSourceConnectorConfig(props);
        }
        catch (Exception e) {
            throw new ConnectException("Couldn't start MTConnectSourceConnector due to configuration error " + e);
        }
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> agentURLs = config.getList(AGENT_URLS);

        if (agentURLs == null) {
            throw new ConfigException("There must be at least one Agent URL");
        }

        if (agentURLs.size() != maxTasks) {
            throw new ConfigException("- max.tasks - must be equal to " +
                    "the size of the - agent urls -.");
        }

        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (String url : agentURLs) {
            Map<String, String> taskConf = new HashMap<>(configProperties);
            taskConf.put(AGENT_URL, url);
            taskConfigs.add(taskConf);
        }

        return taskConfigs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MTConnectSourceTask.class;
    }

    @Override
    public void stop() {
        log.info("Connector is stopping.");
    }

    @Override
    public ConfigDef config() {
        return MTConnectSourceConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }
}
