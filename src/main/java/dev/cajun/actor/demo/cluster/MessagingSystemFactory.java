package dev.cajun.actor.demo.cluster;

import com.cajunsystems.runtime.cluster.DirectMessagingSystem;
import dev.cajun.actor.demo.metastore.InfinispanMetadataStore;
import org.infinispan.manager.EmbeddedCacheManager;

public class MessagingSystemFactory {
    public static String MESSAGING_NODE_KEY_PREFIX = "cajun/messaging/";

    public static Builder builder() {
        return new Builder();
    }

    private static DirectMessagingSystem create(EmbeddedCacheManager cacheManager, String bindAddr, int port) {
        String nodeName = cacheManager.getAddress().toString();
        cacheManager.getCache(InfinispanMetadataStore.CACHE_NAME)
                .put(MESSAGING_NODE_KEY_PREFIX + nodeName, String.format("%s:%d", bindAddr, port));
        return new DirectMessagingSystem(nodeName, port);
    }

    public static class Builder {
        private EmbeddedCacheManager cacheManager;
        private String bindAddress;
        private int port = -1;

        public Builder cacheManager(EmbeddedCacheManager cacheManager) {
            this.cacheManager = cacheManager;
            return this;
        }

        public Builder bindAddress(String bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public DirectMessagingSystem build() {
            if (cacheManager == null || bindAddress == null || port == -1)
                throw new IllegalArgumentException("Required arguments missed: cache manager, bind address, or port!");

            return create(cacheManager, bindAddress, port);
        }
    }
}
