package in.oogway.plumbox.launcher.streaming;

// A Bean class for the format of the stream produced by a Pulsar Producer.
public class StreamSchema {

    private int id;
    private String name;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public StreamSchema() {}
}
