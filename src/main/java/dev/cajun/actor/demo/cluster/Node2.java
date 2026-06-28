package dev.cajun.actor.demo.cluster;

import com.cajunsystems.cluster.ClusterActorSystem;
import com.cajunsystems.cluster.MetadataStore;
import com.cajunsystems.runtime.cluster.DirectMessagingSystem;
import com.cajunsystems.runtime.cluster.EtcdMetadataStore;

import java.util.concurrent.ExecutionException;

public class Node2 {
    public void run() throws ExecutionException, InterruptedException {
        MetadataStore metadataStore1 = new EtcdMetadataStore("http://etcd-host:2379");
        DirectMessagingSystem messagingSystem1 = new DirectMessagingSystem("node1", 8080);
        messagingSystem1.addNode("node2", "node2-host", 8080);
        ClusterActorSystem system1 = new ClusterActorSystem("node1", metadataStore1, messagingSystem1);
        system1.start().get();
    }
}
