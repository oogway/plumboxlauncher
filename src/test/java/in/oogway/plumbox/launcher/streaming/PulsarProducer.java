package in.oogway.plumbox.launcher.streaming;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;

import java.util.concurrent.TimeUnit;

public class PulsarProducer implements Runnable {

    private Client client;
    private String topic;
    private Producer<StreamSchema> producer;
    private volatile boolean execute;

    public PulsarProducer(String topic) throws PulsarClientException, JsonProcessingException {
        client = new Client();
        producer = createProducer(topic);
        this.topic = topic;
    }

    private Producer<StreamSchema> createProducer(String topic) throws PulsarClientException, JsonProcessingException {
        return client.getPulsarClient().newProducer(JSONSchema.of(StreamSchema.class))
                .topic(topic)
                .create();
    }

    public void sendMessage(StreamSchema ss) {
        producer.sendAsync(ss).thenAccept(msgId -> {
            System.out.printf("\nPulsar Producer: Message with ID %s successfully sent on Topic %s",
                    msgId, this.topic);
        });
    }

    // todo add exceptionally().
    public void close(){
        producer.closeAsync()
                .thenRun(() -> System.out.println("\nPulsar Producer closed\nWaiting for the Ingester to terminate."));
        this.execute = false;
    }

    @Override
    public void run() {
        this.execute = true;
        while (this.execute) {
            try {
                System.out.println("\nPulsar Producer started\n");
                StreamSchema ss = null;

                for( int i=1; i<=5; i++){
                    Thread.sleep(3000);
                    ss = new StreamSchema();
                    ss.setId(i);
                    ss.setName("Hello from Pulsar - " + i);

                    this.sendMessage(ss);
                }

                this.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
