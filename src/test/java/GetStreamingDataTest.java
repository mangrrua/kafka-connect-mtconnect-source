import connect.models.ResponseModel;
import connect.mtconnect.AgentEndpoint;
import connect.mtconnect.AgentHttpClient;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.Optional;

public class GetStreamingDataTest {

    /**
     * You can use this method to get streaming data from the agent to test connector task
     * @param args nothing
     */
    public static void main(String[] args) {
        String agentUrl = "http://mtconnect.mazakcorp.com:5611";
        AgentEndpoint agentEndpoint = new AgentEndpoint(agentUrl, 1000, 10, 5000);
        AgentHttpClient agentHttpClient = new AgentHttpClient(agentEndpoint, 15000, 15000);

        while(true) {
            try {
                if (!agentHttpClient.isConnect()) {
                    // if you want to get data from the beginning of the agent buffer,
                    // set from variable to "null"
                    String from = agentHttpClient.getNextSequenceUsingCurrentRequest();
                    agentHttpClient.startToReceiveData(from);
                }

                Optional<ResponseModel> optionalResponse = agentHttpClient.getNextChunkIfExist();
                if (optionalResponse.isPresent()) {
                    ResponseModel model = optionalResponse.get();
                    System.out.println(new String(model.getContent()));
                }
            } catch (IOException | XMLStreamException ioe) {
                ioe.printStackTrace();
                agentHttpClient.cleanResourcesIfExist();
            }
        }
    }
}
