package connect.models;

import java.util.Map;

public class ResponseModel {

    private Map<String, String> sourcePartition;
    private Map<String, String> sourceOffset;
    private String content;

    public ResponseModel(Map<String, String> sourcePartition, Map<String, String> sourceOffset, String content) {
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.content = content;
    }

    public Map<String, String> getSourceOffset() {
        return sourceOffset;
    }

    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    public String getContent() {
        return content;
    }
}
