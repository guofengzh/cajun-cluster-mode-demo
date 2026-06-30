package dev.cajun.actor.demo;

import com.cajunsystems.Pid;
import dev.cajun.actor.demo.cluster.Node;

public class App1 {
  static void main(String[] args) throws Exception {

    String bindAddress = "127.0.0.1";
    int port = 8081;

    Node node = Node.builder()
            .bindAddress(bindAddress)
            .port(port)
            .build();

    // Create actors as usual
    Pid actor = node.getActorSystem().register(MyActor.class, "my-actor");

    // Send messages as usual
    TestMessage message = new TestMessage("Hello from Node1!");
    actor.tell(message);
  }
}
