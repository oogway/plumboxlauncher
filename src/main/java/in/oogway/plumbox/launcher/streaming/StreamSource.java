package in.oogway.plumbox.launcher.streaming;

import in.oogway.plumbox.launcher.Source;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;

/** @param <T> Generic type for stream received from Pulsar's topic.
 *
 * StreamSource is an interface which provides a load method.
 * This load method accepts a JavaStreamingContext and a HashMap of args
 * Args: Pulsar URL, Pulsar Topic name, Pulsar Subscription name.
 *
 * */
public interface StreamSource<T> extends Source {
    JavaReceiverInputDStream<T> load(JavaStreamingContext jssc, HashMap<String, Object> args);
    /*
     * Starts the streaming which receives the data, applies transformations on it and saves to Sink.
     */
    default void startStreaming(JavaStreamingContext jssc, long timeout) throws InterruptedException {
        jssc.start();
        jssc.awaitTerminationOrTimeout(timeout);
    }

    /*
     * Stops the streaming process.
     */
    default void stopStreaming(JavaStreamingContext jssc){
        jssc.stop();
    }
}
