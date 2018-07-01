package in.oogway.plumbox.launcher.streaming;

import in.oogway.plumbox.launcher.streaming.StreamSource;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.spark.SparkStreamingPulsarReceiver;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import java.util.HashMap;

/*
* Implements the StreamSource interface.
* A Pulsar Source.
* */
public class PulsarSource implements StreamSource<byte[]> {

    // Load method:
    /*
    * 1. Creates a Client and Consumer configuration.
    * 2. Starts a receiver stream for the pulsar url, topic and subscription.
    *  Ex: String url = "pulsar://localhost:6650/";
           String topic = "persistent://public/default/topic1";
           String subs = "sub1";
    */
    @Override
    public JavaReceiverInputDStream<byte[]> load(JavaStreamingContext jssc, HashMap args) {

        ClientConfiguration clientConf = new ClientConfiguration();
        ConsumerConfiguration consConf = new ConsumerConfiguration();

        return jssc.receiverStream(
                (Receiver<byte[]>) new SparkStreamingPulsarReceiver(
                        clientConf, consConf, (String) args.get("url"),
                        (String) args.get("topic"), (String) args.get("subscription")
                ));
    }

    @Override
    public Dataset<Row> load(SparkSession ss, HashMap<String, Object> arguments) {
        return null;
    }

    @Override
    public Object getLastWatermark(Dataset<Row> batch) {
        return null;
    }

    @Override
    public void setWatermark(Object mark) {

    }
}
