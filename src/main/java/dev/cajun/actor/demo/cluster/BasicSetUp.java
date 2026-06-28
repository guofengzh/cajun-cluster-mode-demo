package dev.cajun.actor.demo.cluster;

import com.cajunsystems.Pid;
import com.cajunsystems.cluster.ClusterActorSystem;
import com.cajunsystems.cluster.MessagingSystem;
import com.cajunsystems.cluster.MetadataStore;
import com.cajunsystems.runtime.cluster.DirectMessagingSystem;
import com.cajunsystems.runtime.cluster.EtcdMetadataStore;

import java.util.concurrent.ExecutionException;

public class BasicSetUp {
    public void run() throws ExecutionException, InterruptedException {
        // Create a metadata store (using etcd)
        MetadataStore metadataStore = new EtcdMetadataStore("http://localhost:2379");

       // Create a messaging system (using direct TCP)
        MessagingSystem messagingSystem = new DirectMessagingSystem("system1", 8080);

       // Create a cluster actor system
        ClusterActorSystem system = new ClusterActorSystem("system1", metadataStore, messagingSystem);

       // Start the system
        system.start().get();

       // Create actors as usual
        Pid actor = system.register(MyActor.class, "my-actor");

       // Send messages as usual
        actor.tell("Hello, actor!");

        // Shut down the system when done
        system.stop().get();
    }

}
