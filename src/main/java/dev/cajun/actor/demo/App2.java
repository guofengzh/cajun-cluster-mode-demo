package dev.cajun.actor.demo;

import com.cajunsystems.Pid;
import com.cajunsystems.cluster.ClusterActorSystem;
import com.cajunsystems.cluster.DeliveryGuarantee;
import dev.cajun.actor.demo.cluster.Node;

public class App2 {
  static void main(String[] args) throws Exception {

    String bindAddress = "127.0.0.1";
    int port = 8082;

    Node node = Node.builder()
            .bindAddress(bindAddress)
            .port(port)
            .build();
    ClusterActorSystem actorSystem = node.getActorSystem();

    // Send messages as usual
    TestMessage message = new TestMessage("Hello from Node2!");
    actorSystem.routeMessage("my-actor", message, DeliveryGuarantee.EXACTLY_ONCE);

    Pid pid = new Pid("my-actor", actorSystem);
    pid.tell(new TestMessage("Told form Node2!"));
  }
}
