package connect.mtconnect;

import connect.models.ResponseMetadata;
import org.junit.Test;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.Assert.*;

public class XMLUtilsTest {

    private XMLUtils xmlUtils = new XMLUtils();

    private String responseWithDataStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<MTConnectStreams xmlns:m=\"urn:mtconnect.org:MTConnectStreams:1.4\" " +
            "xmlns=\"urn:mtconnect.org:MTConnectStreams:1.4\" " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xmlns:x=\"urn:mazakusa.com:MazakStreams:1.4\" " +
            "xsi:schemaLocation=\"urn:mazakusa.com:MazakStreams:1.4 /schemas/MazakStreams_1.4.xsd\">\n" +
            "  <Header creationTime=\"2019-08-14T16:28:00Z\" sender=\"DMZ-MTCNCT\" instanceId=\"1564950676\" " +
            "version=\"1.4.0.12\" bufferSize=\"131072\" nextSequence=\"381153\" firstSequence=\"380404\" " +
            "lastSequence=\"511475\"/>\n" +
            "  <Streams>\n" +
            "    <DeviceStream name=\"Mazak\" uuid=\"M80104K162N\">\n" +
            "      <ComponentStream component=\"Rotary\" name=\"C\" componentId=\"c\">\n" +
            "        <Samples>\n" +
            "          <Load dataItemId=\"Sload\" timestamp=\"2019-08-12T18:30:26.604408Z\" " +
            "sequence=\"381151\">0</Load>\n" +
            "          <RotaryVelocity dataItemId=\"Srpm\" timestamp=\"2019-08-12T18:30:26.604408Z\" " +
            "sequence=\"381150\" subType=\"ACTUAL\">0</RotaryVelocity>\n" +
            "        </Samples>\n" +
            "        <Condition>\n" +
            "          <Normal dataItemId=\"Sload_cond\" timestamp=\"2019-08-12T18:30:26.604408Z\" " +
            "sequence=\"381152\" type=\"LOAD\"/>\n" +
            "        </Condition>\n" +
            "      </ComponentStream>\n" +
            "    </DeviceStream>\n" +
            "  </Streams>\n" +
            "</MTConnectStreams>";

    private String emptyResponseStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<MTConnectStreams xmlns:m=\"urn:mtconnect.org:MTConnectStreams:1.4\" " +
            "xmlns=\"urn:mtconnect.org:MTConnectStreams:1.4\" " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xmlns:x=\"urn:mazakusa.com:MazakStreams:1.4\" " +
            "xsi:schemaLocation=\"urn:mazakusa.com:MazakStreams:1.4 /schemas/MazakStreams_1.4.xsd\">\n" +
            "  <Header creationTime=\"2019-08-14T16:21:33Z\" sender=\"DMZ-MTCNCT\" instanceId=\"1564950676\" " +
            "version=\"1.4.0.12\" bufferSize=\"131072\" nextSequence=\"511429\" firstSequence=\"380357\" " +
            "lastSequence=\"511428\"/>\n" +
            "  <Streams/>\n" +
            "</MTConnectStreams>";
    
    private String errorDocumentResStr = "<MTConnectError " +
            "xmlns:m=\"urn:mtconnect.org:MTConnectError:1.4\" " +
            "xmlns=\"urn:mtconnect.org:MTConnectError:1.4\" " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xsi:schemaLocation=\"urn:mtconnect.org:MTConnectError:1.4 " +
            "http://schemas.mtconnect.org/schemas/MTConnectError_1.4.xsd\">\n" +
            "<Header creationTime=\"2019-08-14T16:36:07Z\" sender=\"DMZ-MTCNCT\" instanceId=\"1564950676\" " +
            "version=\"1.4.0.12\" bufferSize=\"131072\"/>\n" +
            "<Errors>\n" +
            "<Error errorCode=\"OUT_OF_RANGE\">'from' must be less than or equal to 511536.</Error>\n" +
            "</Errors>\n" +
            "</MTConnectError>";

    private InputStream emptyResIs = new ByteArrayInputStream(emptyResponseStr.getBytes());
    private InputStream docWithDataIs = new ByteArrayInputStream(responseWithDataStr.getBytes());
    private InputStream errorDocIs = new ByteArrayInputStream(errorDocumentResStr.getBytes());

    @Test
    public void documentMetadataControl() throws XMLStreamException {
        ResponseMetadata metadata = xmlUtils.getMetadataOfResponse(docWithDataIs);
        assertEquals(metadata.getInstanceId(), "1564950676");
        assertEquals(metadata.getFirstSequence(), "380404");
        assertEquals(metadata.getNextSequence(), "381153");
        assertTrue(metadata.isDataExist());
    }

    @Test
    public void isDataExistInResponseDoc() throws XMLStreamException {
        ResponseMetadata metadata = xmlUtils.getMetadataOfResponse(emptyResIs);
        assertFalse(metadata.isDataExist());
    }

    @Test
    public void checkResponseErrorDocument() throws XMLStreamException {
        boolean result = xmlUtils.isErrorDocument(errorDocIs);
        assertTrue(result);
    }

}
