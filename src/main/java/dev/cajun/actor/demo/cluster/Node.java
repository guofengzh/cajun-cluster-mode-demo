package dev.cajun.actor.demo.cluster;

import com.cajunsystems.cluster.ClusterActorSystem;
import com.cajunsystems.cluster.MetadataStore;
import com.cajunsystems.runtime.cluster.DirectMessagingSystem;
import dev.cajun.actor.demo.metastore.InfinispanMetadataStore;
import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Node {
    private static final Logger logger = LoggerFactory.getLogger(Node.class);

    private ClusterActorSystem system;
    private EmbeddedCacheManager cacheManager;

    public ClusterActorSystem getActorSystem() {
        return system;
    }

    public EmbeddedCacheManager getCacheManager() {
        return cacheManager;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Node() {
    }

    private void initializeActorSystem(String bindAddress, int port) throws ExecutionException, InterruptedException, IOException {
        cacheManager = new DefaultCacheManager(InfinispanMetadataStore.class.getResourceAsStream("/infinispan.xml"));
        cacheManager.addListenerAsync(new NodeLeaveListener());

        String nodeName = cacheManager.getAddress().toString();

        MetadataStore metadataStore = new InfinispanMetadataStore(cacheManager, nodeName);
        DirectMessagingSystem messagingSystem = MessagingSystemFactory.builder()
                .cacheManager(cacheManager)
                .bindAddress(bindAddress)
                .port(port)
                .build();

        Cache<String, String> cache = cacheManager.getCache(InfinispanMetadataStore.CACHE_NAME);
        initializeMessagingSystem(messagingSystem, cache);

        cache.put(MessagingSystemFactory.MESSAGING_NODE_KEY_PREFIX + nodeName, String.format("%s:%d", bindAddress, port));

        cache.addListenerAsync(new MessagingPartnerListener(messagingSystem, nodeName));

        system = new ClusterActorSystem(nodeName, metadataStore, messagingSystem);
        system.start().get();
    }

    private void initializeMessagingSystem(DirectMessagingSystem messagingSystem, Cache<String, String> cache) {
        List<String> keys = cache.keySet().stream()
                .filter(key -> key.startsWith(MessagingSystemFactory.MESSAGING_NODE_KEY_PREFIX))
                .toList();

        keys.forEach(key-> {
            String value = cache.get(key);
            String[] hostPort = value.split(":");
            String nodeId = key.substring(MessagingSystemFactory.MESSAGING_NODE_KEY_PREFIX.length());
            messagingSystem.addNode(nodeId, hostPort[0], Integer.parseInt(hostPort[1]));
        });
    }

    public static class Builder {
        private ClusterActorSystem system;
        private EmbeddedCacheManager cacheManager;
        private String bindAddress;
        private int port = -1;

        public Builder bindAddress(String bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Node build() throws Exception {
            Node node = new Node();
            node.initializeActorSystem(bindAddress, port);
            return node;
        }
    }

    @Listener
    public static class NodeLeaveListener {

        @ViewChanged
        public void viewChanged(ViewChangedEvent event) {
            logger.debug("Cluster view changed!");
            logger.debug("Old members: {}", event.getOldMembers());
            logger.debug("New members: {}", event.getNewMembers());

            // Compare members to deduce which node left
            if (event.getOldMembers().size() > event.getNewMembers().size()) {
                logger.debug("A node has left the cluster.");

                CompletableFuture.runAsync(() -> {
                    // Find nodes that left
                    event.getOldMembers().stream()
                            .filter(member -> !event.getNewMembers().contains(member))
                            .forEach(departedNode -> {
                                String nodeId = departedNode.toString();
                                Cache<String, String> cache = event.getCacheManager().getCache(InfinispanMetadataStore.CACHE_NAME);
                                // if this node is the leader, remove it from the cache
                                cache.removeAsync(InfinispanMetadataStore.LEADER_KEY, nodeId);
                            });
                });
            }
        }
    }

    @Listener(clustered = true, observation = Listener.Observation.POST)
    private static class MessagingPartnerListener {
        private final DirectMessagingSystem messagingSystem;
        private final String currentNode;
        private final String MESSAGING_PARTNER_ENTRY_KEY_PREFIX = "cajun/messaging/";

        public MessagingPartnerListener(DirectMessagingSystem messagingSystem, String currentNode) {
            this.messagingSystem = messagingSystem;
            this.currentNode = currentNode;
        }

        private void setupMessagingTarget(String key, String value) {
            String currentNodeKey = MESSAGING_PARTNER_ENTRY_KEY_PREFIX + currentNode;
            if (key.startsWith(MESSAGING_PARTNER_ENTRY_KEY_PREFIX) && !key.equals(currentNodeKey)) {
                // register partners for messaging
                String[] hostPort = value.split(":");
                String nodeId = key.substring(MESSAGING_PARTNER_ENTRY_KEY_PREFIX.length());
                this.messagingSystem.addNode(nodeId, hostPort[0], Integer.parseInt(hostPort[1]));
                logger.info("register the partner: event.getNewValue()");
            }
        }

        @CacheEntryCreated
        public void onCreated(CacheEntryCreatedEvent<String, String> event) {
            if (event.isPre()) return;
            setupMessagingTarget(event.getKey(), event.getValue());
        }

        @CacheEntryModified
        public void onModified(CacheEntryModifiedEvent<String, String> event) {
            if (event.isPre()) return;
            setupMessagingTarget(event.getKey(), event.getNewValue());
        }

        @CacheEntryRemoved
        public void onRemoved(CacheEntryRemovedEvent<String, String> event) {
            String key = event.getKey();
            String currentNodeKey = MESSAGING_PARTNER_ENTRY_KEY_PREFIX + currentNode;
            if (key.startsWith(MESSAGING_PARTNER_ENTRY_KEY_PREFIX) && !key.equals(currentNodeKey)) {
                // TODO: This partner cannot be removed from the messaging system.
            }
        }
    }
}
