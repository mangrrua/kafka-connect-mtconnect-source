package connect.mtconnect;

import connect.exceptions.MTConnectAgentException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ChunkStreamReader {

    private byte LF = 0x0A; // "\n"
    private byte CR = 0x0D; // "\r"
    private BufferedInputStream bis;
    private String boundary;
    private boolean hasNext;

    public ChunkStreamReader(BufferedInputStream bis, String boundary){
        this.bis = bis;
        this.boundary = boundary;
        hasNext = true;
    }

    private String readLine() throws IOException {
        StringBuilder builder = new StringBuilder();

        for (;;) {
            // if there is no available data, thread waits in read() method until the data received
            // if agent does not sends the any data(includes heartbeat), an exception will be thrown
            // due to when read timeout limit exceed
            int i = bis.read();
            if (i == LF) {
                return builder.toString();
            }

            if (i != CR) {
                builder.append((char)i);
            }
        }
    }

    private void readUntilBoundary() throws IOException {
        for (;;) {
            String line = readLine();
            if (line.equals(boundary)) {
                break;
            } else if (line.equals(boundary + "--")) {
                hasNext = false;
                break;
            }
        }
    }

    private Map<String, String> readHeader() throws IOException {
        Map<String, String> headers = new HashMap<>();
        String line = readLine();
        while (line.trim().length() != 0){
            if (line.startsWith("Content-")){
                String[] parts = line.split(":");
                headers.put(parts[0], parts[1].trim());
            }
            line = readLine();
        }
        return headers;
    }

    private byte[] readChunk(Integer contentLength) throws IOException {
        byte[] buffer = new byte[contentLength];

        int bytes = 0;
        while (bytes < contentLength) {
            bytes += bis.read(buffer, bytes, contentLength - bytes);
        }

        return buffer;
    }

    /**
     * Read next chunk from the connection. If stream ends, or any error
     * occurs while reading response, connector task will be stopped.
     * @return received XML document(chunk) as byte[] format
     * @throws IOException if connection fails
     */
    public byte[] getNextChunk() throws IOException {
        synchronized (this) {
            readUntilBoundary();
            if (hasNext) {
                Map<String, String> header = readHeader();
                String contentLength = header.get("Content-length");
                try {
                    int length = Integer.parseInt(contentLength);
                    return readChunk(length);
                } catch (NumberFormatException e) {
                    throw new MTConnectAgentException("{Content length = " + contentLength + "} of received chunk " +
                            "was not successfully casted to integer.");
                }
            } else {
                throw new MTConnectAgentException("Stream was ended");
            }
        }
    }

    public void close() throws IOException {
        synchronized (this) {
            if (bis != null){
                bis.close();
            }
        }
    }
}

