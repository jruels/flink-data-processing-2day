package example;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class SimpleFlinkSocket {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // open socket to read from that we start with netcat
        DataStream<String> lines = env.socketTextStream("localhost", 9999);

	lines.print();

        // Execute the Flink program
        env.execute();
    }
}