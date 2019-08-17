package connect;

import connect.configs.MTConnectSourceTaskConfig;
import connect.models.ResponseModel;
import connect.mtconnect.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.*;

public class MTConnectSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(MTConnectSourceTask.class);

    private MTConnectSourceTaskConfig config;
    private AgentHttpClient agentHttpClient;

    @Override
    public void start(Map<String, String> props) {
        config = new MTConnectSourceTaskConfig(props);

        String agentUrl = config.getUrl();
        int interval = config.getInterval();
        int count = config.getCount();
        int heartbeat = config.getHeartbeat();
        int connectTimeout = config.getConnectTimeout();
        int readTimeout = config.getReadTimeout();

        AgentEndpoint agentEndpoint = new AgentEndpoint(agentUrl, interval, count, heartbeat);
        agentHttpClient = new AgentHttpClient(agentEndpoint, connectTimeout, readTimeout);

        log.info("Task is starting with props={}", props);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            // connection will be checked each time, because agents sometimes downs or fails
            if (!agentHttpClient.isConnect()) {
                String from;
                if (config.isFromLatest()) {
                    from = agentHttpClient.getNextSequenceUsingCurrentRequest();
                } else {
                    from = agentHttpClient.getNextSequenceUsingOffsets(context);
                }

                agentHttpClient.startToReceiveData(from);
            }

            // the following section will be triggered only if connection is successful and
            // not occurred any error while parsing received response
            Optional<ResponseModel> optionalResponse = agentHttpClient.getNextChunkIfExist();
            if (optionalResponse.isPresent()) {
                ResponseModel model = optionalResponse.get();
                SourceRecord record = generateMTConnectRecord(model);
                return Collections.singletonList(record);
            }
        } catch (IOException ioe) {
            // IOException wil be triggered if any error occurs to connection between client and agent
            log.error("Connection failed with {}. Trying to reconnect again. {}", config.getUrl(), ioe.toString());
            agentHttpClient.cleanResourcesIfExist();

            SourceRecord record = generateUnreachableAgentRecord(ioe.getMessage());
            return Collections.singletonList(record);
        } catch (XMLStreamException xse) {
            throw new ConnectException("Could not parse received response from " +
                    config.getUrl() + " due to " + xse.toString());
        }

        return null;
    }

    protected SourceRecord generateMTConnectRecord(ResponseModel data) {
        return new SourceRecord(
                data.getSourcePartition(),
                data.getSourceOffset(),
                config.getTopic(),
                null,
                Schema.STRING_SCHEMA,
                config.getUrl(),
                Schema.STRING_SCHEMA,
                data.getContent());
    }

    protected SourceRecord generateUnreachableAgentRecord(String errorMessage) {
        long requestTimestamp = System.currentTimeMillis();
        JSONObject jsonValue = new JSONObject();
        jsonValue.put("agent_url", config.getUrl());
        jsonValue.put("error_message", errorMessage);
        jsonValue.put("request_timestamp", requestTimestamp);

        return new SourceRecord(
                null,
                null,
                config.getUnreachableTopic(),
                null,
                Schema.STRING_SCHEMA,
                config.getUrl(),
                Schema.STRING_SCHEMA,
                jsonValue.toString());
    }

    @Override
    public void stop() {
        log.info("Task for {} is stopping", config.getUrl());
        agentHttpClient.cleanResourcesIfExist();
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }
}
