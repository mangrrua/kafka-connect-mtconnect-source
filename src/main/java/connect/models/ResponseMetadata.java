package connect.models;

import java.util.HashMap;
import java.util.Map;

public class ResponseMetadata {

    public static final String INSTANCE_ID = "instance_id";
    public static final String NEXT_SEQUENCE = "next_sequence";
    public static final String FIRST_SEQUENCE = "first_sequence";

    private String nextSequence;
    private String instanceId;
    private String firstSequence;
    private boolean isDataExistInTheResponse;

    public ResponseMetadata(String nextSequence,
                            String instanceId,
                            String firstSequence,
                            boolean isDataExistInTheResponse) {
        this.nextSequence = nextSequence;
        this.instanceId = instanceId;
        this.firstSequence = firstSequence;
        this.isDataExistInTheResponse = isDataExistInTheResponse;
    }

    public String getNextSequence() {
        return nextSequence;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getFirstSequence() {
        return firstSequence;
    }

    public Map<String, String> getAsMap() {
        Map<String, String> sourceOffset = new HashMap<>();
        sourceOffset.put(INSTANCE_ID, instanceId);
        sourceOffset.put(NEXT_SEQUENCE, nextSequence);
        sourceOffset.put(FIRST_SEQUENCE, firstSequence);

        return sourceOffset;
    }

    public boolean isDataExist() {
        return isDataExistInTheResponse;
    }
}

