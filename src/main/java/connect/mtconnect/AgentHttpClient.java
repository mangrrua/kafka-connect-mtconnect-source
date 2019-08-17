package connect.mtconnect;

import connect.models.ResponseMetadata;
import connect.models.ResponseModel;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static connect.models.ResponseMetadata.INSTANCE_ID;
import static connect.models.ResponseMetadata.NEXT_SEQUENCE;

public class AgentHttpClient {
    private static final Logger log = LoggerFactory.getLogger(AgentHttpClient.class);
    private static final String AGENT_URL = "agent_url";
    private static final String MULTIPART_X_MIXED_REPLACE = "multipart/x-mixed-replace";

    private XMLUtils xmlUtils;
    private ChunkStreamReader streamReader;
    private HttpURLConnection sampleRequestConnection;
    private AgentEndpoint agentEndpoint;
    private int connectTimeout;
    private int readTimeout;

    public AgentHttpClient(AgentEndpoint agentEndpoint, int connectTimeout, int readTimeout) {
        this.xmlUtils = new XMLUtils();
        this.agentEndpoint = agentEndpoint;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
    }

    /**
     * If connection is successful with the agent and not occurs any error while getting metadata(xml parsing) from
     * the received MTConnect XML response that means everything is ok and we can read data from the agent.
     * @return true if reader is created
     */
    public boolean isConnect() {
        return streamReader != null;
    }

    private HttpURLConnection connect(String preparedUrl) throws IOException {
        try {
            URL url = new URL(preparedUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("Connection", "close");
            connection.setConnectTimeout(connectTimeout);
            connection.setReadTimeout(readTimeout);
            connection.connect();

            if (connection.getResponseCode() != 200) {
                String errorMessage = String.format(
                        "%s - Response code: %s. Response message %s",
                        agentEndpoint.getUrl(),
                        connection.getResponseCode(),
                        connection.getResponseMessage());
                throw new IOException(errorMessage);
            }

            return connection;
        } catch (MalformedURLException e) {
            String errorMessage = String.format(
                    "Url %s invalid format. %s",
                    preparedUrl, e.toString());
            throw new ConnectException(errorMessage);
        }
    }

    public void startToReceiveData(String from) throws IOException, XMLStreamException {
        String sampleUrl = agentEndpoint.constructSampleUrl(from);
        sampleRequestConnection = connect(sampleUrl);

        // example 'Content-Type': multipart/x-mixed-replace;boundary=anything
        String[] contentType = sampleRequestConnection.getContentType().split(";");
        InputStream is = sampleRequestConnection.getInputStream();

        if (!contentType[0].equals(MULTIPART_X_MIXED_REPLACE)) {
            boolean isErrorDoc = xmlUtils.isErrorDocument(is);
            // clear resources
            is.close();
            disconnect();
            if (isErrorDoc) {
                log.warn("Task tried to get data from {}, but agent does not have data with nextSequence " +
                        "that received from the saved offsets.", agentEndpoint.getUrl());
                log.info("Data will be received from the beginning of the buffer of {}.", agentEndpoint.getUrl());
                startToReceiveData(null);
            } else {
                throw new ConnectException("Agent does not support " + MULTIPART_X_MIXED_REPLACE);
            }
        }

        String boundary = contentType[1].split("=")[1];
        boundary = boundary.startsWith("--") ? boundary : "--" + boundary;
        BufferedInputStream bis = new BufferedInputStream(is);

        streamReader = new ChunkStreamReader(bis, boundary);
        log.info("Successfully connected to the {}. Starting to receive data.", agentEndpoint.getUrl());
    }

    private ResponseMetadata getCurrentMetadataOfAgent() throws IOException, XMLStreamException {
        String currentUrl = agentEndpoint.constructCurrentUrl();
        HttpURLConnection conn = connect(currentUrl);
        InputStream is = conn.getInputStream();

        ResponseMetadata metadata = xmlUtils.getMetadataOfResponse(is);

        is.close();
        conn.disconnect();

        return metadata;
    }

    public String getNextSequenceUsingCurrentRequest() throws IOException, XMLStreamException {
        log.info("Data will be received from the ending of the buffer of {}.", agentEndpoint.getUrl());
        ResponseMetadata metadata = getCurrentMetadataOfAgent();
        return metadata.getNextSequence();
    }

    public String getNextSequenceUsingOffsets(SourceTaskContext sourceTaskContext) throws IOException, XMLStreamException{
        Map<String, Object> partition = Collections.singletonMap(AGENT_URL, agentEndpoint.getUrl());
        Map<String, Object> offsets = sourceTaskContext.offsetStorageReader().offset(partition);
        ResponseMetadata currentMetadata = getCurrentMetadataOfAgent();

        String from = null;
        if (offsets != null) {
            String instanceId = (String) offsets.get(INSTANCE_ID);
            String nextSequence = (String) offsets.get(NEXT_SEQUENCE);

            log.info("Offsets found for {}", agentEndpoint.getUrl());

            // check if agent has restarted
            if (instanceId.equals(currentMetadata.getInstanceId())) {
                int ns = Integer.parseInt(nextSequence);
                int currentFs = Integer.parseInt(currentMetadata.getFirstSequence());
                if (ns > currentFs) {
                    log.info("Data will be received from [instanceId: {} - nextSequence: {}] of the buffer of {}",
                            instanceId, nextSequence, agentEndpoint.getUrl());
                    from = nextSequence;
                }
            }
        }

        if (from == null) {
            log.info("Data will be received from the beginning of the buffer of {}", agentEndpoint.getUrl());
        }

        return from;
    }

    /**
     * MTConnect agent sends a empty document if there is no new data in the buffer(every heartbeat ms).
     * Received empty document has the same nextSequence id with the last received document.
     * Thus, we do not need to send empty response to the Kafka.
     * @return Response that received from the MTConnect agent
     * @throws IOException if connection fails
     * @throws XMLStreamException if any error occurs while getting metadata from the response
     */
    public Optional<ResponseModel> getNextChunkIfExist() throws IOException, XMLStreamException {
        byte[] receivedChunk = streamReader.getNextChunk();
        InputStream is = new ByteArrayInputStream(receivedChunk);
        ResponseMetadata metadata = xmlUtils.getMetadataOfResponse(is);
        if (metadata.isDataExist()) {
            Map<String, String> sourcePartition = Collections.singletonMap(AGENT_URL, agentEndpoint.getUrl());
            Map<String, String> sourceOffset = metadata.getAsMap();
            String value = new String(receivedChunk);
            ResponseModel response = new ResponseModel(sourcePartition, sourceOffset, value);
            return Optional.of(response);
        }

        return Optional.empty();
    }

    public void cleanResourcesIfExist() {
        closeReader();
        disconnect();
    }

    private void closeReader() {
        if (streamReader != null) {
            try {
                log.info("Closing the connection with {}", agentEndpoint.getUrl());
                streamReader.close();
            } catch (IOException e) {
                log.warn("Ignoring error closing connection", e);
            } finally {
                streamReader = null;
            }
        }
    }

    private void disconnect() {
        if (sampleRequestConnection != null) {
            sampleRequestConnection.disconnect();
            sampleRequestConnection = null;
        }
    }
}
