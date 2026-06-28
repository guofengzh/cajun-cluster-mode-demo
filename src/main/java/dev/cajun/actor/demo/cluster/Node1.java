package dev.cajun.actor.demo.cluster;

import com.cajunsystems.cluster.ClusterActorSystem;
import com.cajunsystems.cluster.MetadataStore;
import com.cajunsystems.runtime.cluster.DirectMessagingSystem;
import com.cajunsystems.runtime.cluster.EtcdMetadataStore;

import java.util.concurrent.ExecutionException;

public class Node1 {
    public void run() throws ExecutionException, InterruptedException {
        MetadataStore metadataStore2 = new EtcdMetadataStore("http://etcd-host:2379");
        DirectMessagingSystem messagingSystem2 = new DirectMessagingSystem("node2", 8080);
        messagingSystem2.addNode("node1", "node1-host", 8080);
        ClusterActorSystem system2 = new ClusterActorSystem("node2", metadataStore2, messagingSystem2);
        system2.start().get();
    }
}
