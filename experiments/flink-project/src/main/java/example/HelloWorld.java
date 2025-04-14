package example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class HelloWorld {

  public static void main(String[] args) throws Exception {
    // set up the stream execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


  env.fromElements(1, 2, 3, 4, 5)
    .map(i -> 2 * i)
    .print();

  env.execute();
  } 
}