package connect.mtconnect;

import connect.exceptions.MTConnectAgentException;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

public class ChunkStreamReaderTest {

    private String chunk = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<MTConnectStreams xmlns:m=\"urn:mtconnect.org:MTConnectStreams:1.4\" " +
            "xmlns=\"urn:mtconnect.org:MTConnectStreams:1.4\" " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xmlns:x=\"urn:mazakusa.com:MazakStreams:1.4\" " +
            "xsi:schemaLocation=\"urn:mazakusa.com:MazakStreams:1.4 /schemas/MazakStreams_1.4.xsd\">\n" +
            "  <Header creationTime=\"2019-08-14T17:13:31Z\" sender=\"DMZ-MTCNCT\" instanceId=\"1564950676\" " +
            "version=\"1.4.0.12\" bufferSize=\"131072\" nextSequence=\"382152\" firstSequence=\"382146\" " +
            "lastSequence=\"513217\"/>\n" +
            "  <Streams>\n" +
            "    <DeviceStream name=\"Mazak\" uuid=\"M80104K162N\">\n" +
            "      <ComponentStream component=\"Path\" name=\"path\" componentId=\"path1\">\n" +
            "        <Events>\n" +
            "          <LineNumber dataItemId=\"linenumber\" timestamp=\"2019-08-12T18:35:16.229975Z\" " +
            "sequence=\"382149\" subType=\"INCREMENTAL\">3</LineNumber>\n" +
            "          <Program dataItemId=\"subprogram\" timestamp=\"2019-08-12T18:35:16.229975Z\" " +
            "sequence=\"382150\" subType=\"x:SUB\">9220</Program>\n" +
            "          <ProgramComment dataItemId=\"subprogram_cmt\" timestamp=\"2019-08-12T18:35:16.229975Z\" " +
            "sequence=\"382151\" subType=\"x:SUB\">TOOL BRK CHECK</ProgramComment>\n" +
            "        </Events>\n" +
            "      </ComponentStream>\n" +
            "    </DeviceStream>\n" +
            "  </Streams>\n" +
            "</MTConnectStreams>\n\n\n";

    private String chunk2 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<MTConnectStreams xmlns:m=\"urn:mtconnect.org:MTConnectStreams:1.4\" " +
            "xmlns=\"urn:mtconnect.org:MTConnectStreams:1.4\" " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xmlns:x=\"urn:mazakusa.com:MazakStreams:1.4\" " +
            "xsi:schemaLocation=\"urn:mazakusa.com:MazakStreams:1.4 /schemas/MazakStreams_1.4.xsd\">\n" +
            "  <Header creationTime=\"2019-08-14T17:13:31Z\" sender=\"DMZ-MTCNCT\" instanceId=\"1564950676\" " +
            "version=\"1.4.0.12\" bufferSize=\"131072\" nextSequence=\"382155\" firstSequence=\"382147\" " +
            "lastSequence=\"513218\"/>\n" +
            "  <Streams>\n" +
            "    <DeviceStream name=\"Mazak\" uuid=\"M80104K162N\">\n" +
            "      <ComponentStream component=\"Linear\" name=\"X\" componentId=\"x\">\n" +
            "        <Samples>\n" +
            "          <AxisFeedrate dataItemId=\"Xfrt\" timestamp=\"2019-08-12T18:35:16.229975Z\" " +
            "sequence=\"382153\">16.48005</AxisFeedrate>\n" +
            "          <Load dataItemId=\"Xload\" timestamp=\"2019-08-12T18:35:16.229975Z\" " +
            "sequence=\"382152\">99</Load>\n" +
            "        </Samples>\n" +
            "        <Events>\n" +
            "          <AxisState dataItemId=\"xaxisstate\" timestamp=\"2019-08-12T18:35:16.229975Z\" " +
            "sequence=\"382154\">TRAVEL</AxisState>\n" +
            "        </Events>\n" +
            "      </ComponentStream>\n" +
            "    </DeviceStream>\n" +
            "  </Streams>\n" +
            "</MTConnectStreams>\n\n\n";

    // simulate the multipart/x-mixed-replace data
    private String multipartData = "--boundary\n" +
            "Content-type: text/xml\n" +
            "Content-length: 1219\n" +
            "\n" +
             chunk +
            "\n" +
            "\n" +
            "--boundary\n" +
            "Content-type: text/xml\n" +
            "Content-length: 1167\n" +
            "\n" +
             chunk2 +
            "\n" +
            "\n" +
            "--boundary--\n" +
            "\n";

    private InputStream is = new ByteArrayInputStream(multipartData.getBytes());
    private BufferedInputStream bis = new BufferedInputStream(is);
    private String boundary = "--boundary";
    private ChunkStreamReader streamReader = new ChunkStreamReader(bis, boundary);


    @Test
    public void checkChunks() throws IOException {
        byte[] result = streamReader.getNextChunk();
        assertEquals(new String(result), chunk);

        byte[] result2 = streamReader.getNextChunk();
        assertEquals(new String(result2), chunk2);

        // end of stream
        try {
            byte[] result3 = streamReader.getNextChunk();
        }
        catch (MTConnectAgentException e) {
            assertEquals(e.getMessage(), "Stream was ended");
        }
    }

}
