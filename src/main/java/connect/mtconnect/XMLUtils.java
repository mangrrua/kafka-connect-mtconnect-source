package connect.mtconnect;

import connect.models.ResponseMetadata;

import javax.xml.stream.*;
import java.io.InputStream;

public class XMLUtils {

    private XMLInputFactory xmlInputFactory;

    public XMLUtils() {
        this.xmlInputFactory = XMLInputFactory.newFactory();
    }

    public ResponseMetadata getMetadataOfResponse(InputStream inStream) throws XMLStreamException {
        XMLStreamReader xmlParser = xmlInputFactory.createXMLStreamReader(inStream);

        String instanceId = null;
        String nextSequence = null;
        String firstSequence = null;
        boolean isDataExist = false;
        boolean isOffsetsReceived = false;

        while (xmlParser.hasNext()) {
            if (xmlParser.next() == XMLStreamConstants.START_ELEMENT) {
                String tagName = xmlParser.getLocalName();

                if (tagName.equals("Header")) {
                    instanceId = xmlParser.getAttributeValue(null, "instanceId");
                    nextSequence = xmlParser.getAttributeValue(null, "nextSequence");
                    firstSequence = xmlParser.getAttributeValue(null, "firstSequence");
                    isOffsetsReceived = true;
                } else if (tagName.equals("ComponentStream")) {
                    // if received response has component stream tag, that means document contains data
                    isDataExist = true;
                }
            }

            // make sure the document has data and offsets information is received
            if (isDataExist && isOffsetsReceived)
                break;
        }
        xmlParser.close();

        return new ResponseMetadata(nextSequence, instanceId, firstSequence, isDataExist);
    }

    public Boolean isErrorDocument(InputStream inStream) throws XMLStreamException {
        XMLStreamReader xmlParser = xmlInputFactory.createXMLStreamReader(inStream);

        boolean isContains = false;
        while (xmlParser.hasNext()) {
            if ((xmlParser.next() == XMLStreamConstants.START_ELEMENT) &&
                    xmlParser.getLocalName().equals("Errors")) {
                isContains = true;
                break;
            }
        }
        xmlParser.close();

        return isContains;
    }
}
