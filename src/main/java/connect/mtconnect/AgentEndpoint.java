package connect.mtconnect;

public class AgentEndpoint {

    private String url;
    private int interval;
    private int count;
    private int heartbeat;

    public AgentEndpoint(String url, int interval, int count, int heartbeat) {
        this.url = url;
        this.interval = interval;
        this.count = count;
        this.heartbeat = heartbeat;
    }

    public String getUrl() {
        return url;
    }

    public String constructCurrentUrl() {
        return url + "/current";
    }

    /**
     * Construct sample url to get data from the agent. If "from" parameter is not specified,
     * that means connector task will be get data from the beginning of the agent buffer.
     * @param from position in the agent buffer
     * @return sample url
     */
    public String constructSampleUrl(String from) {
        if (from == null) {
            String sampleUrl = url + "/sample?interval=%s&count=%s&heartbeat=%s";
            return String.format(
                    sampleUrl,
                    interval,
                    count,
                    heartbeat);
        }
        else {
            String sampleUrl = url + "/sample?from=%s&interval=%s&count=%s&heartbeat=%s";
            return String.format(
                    sampleUrl,
                    from,
                    interval,
                    count,
                    heartbeat);
        }
    }
}
