package dev.cajun.actor.demo.metastore;

import com.cajunsystems.cluster.MetadataStore;
import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Infinispan embedded implementation of the MetadataStore.
 */
public class InfinispanMetadataStore implements MetadataStore {

    private static final String CACHE_NAME = "cajun-metadata-cache";

    private DefaultCacheManager cacheManager;
    private Cache<String, String> cache;

    private final AtomicLong watcherIdGenerator = new AtomicLong(0);
    private final Map<Long, Object> activeWatchers = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Void> connect() {
        return CompletableFuture.runAsync(() -> {
            // Configure embedded clustered mode
            GlobalConfigurationBuilder globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder();
            globalConfig.transport().clusterName("cajun-actor-cluster");

            cacheManager = new DefaultCacheManager(globalConfig.build());

            // Define a distributed synchronous cache configuration
            ConfigurationBuilder cacheConfig = new ConfigurationBuilder();
            cacheConfig.clustering().cacheMode(CacheMode.DIST_SYNC);

            cache = cacheManager.administration()
                    .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
                    .getOrCreateCache(CACHE_NAME, cacheConfig.build());
        });
    }

    @Override
    public CompletableFuture<Void> close() {
        return CompletableFuture.runAsync(() -> {
            if (cacheManager != null) {
                cacheManager.stop();
            }
        });
    }

    @Override
    public CompletableFuture<Void> put(String key, String value) {
        return cache.putAsync(key, value).thenApply(ignore -> null);
    }

    @Override
    public CompletableFuture<Optional<String>> get(String key) {
        return cache.getAsync(key).thenApply(Optional::ofNullable);
    }

    @Override
    public CompletableFuture<Void> delete(String key) {
        return cache.removeAsync(key).thenApply(ignore -> null);
    }

    @Override
    public CompletableFuture<List<String>> listKeys(String prefix) {
        return CompletableFuture.supplyAsync(() ->
                cache.keySet().stream()
                        .filter(key -> key.startsWith(prefix))
                        .collect(Collectors.toList())
        );
    }

    @Override
    public CompletableFuture<Optional<Lock>> acquireLock(String lockName, long ttlSeconds) {
        String lockKey = "lock::" + lockName;
        String lockOwnerId = UUID.randomUUID().toString();

        // Using putIfAbsent with lifespan ensures atomic locking with a strict TTL
        return cache.putIfAbsentAsync(lockKey, lockOwnerId, ttlSeconds, TimeUnit.SECONDS)
                .thenApply(existingValue -> {
                    if (existingValue == null) {
                        // Lock successfully acquired
                        return Optional.of(new InfinispanLock(lockKey, lockOwnerId, ttlSeconds));
                    }
                    // Lock is already held by someone else
                    return Optional.empty();
                });
    }

    @Override
    public CompletableFuture<Long> watch(String key, KeyWatcher watcher) {
        long watchId = watcherIdGenerator.incrementAndGet();
        KeyEventListener listener = new KeyEventListener(key, watcher);

        activeWatchers.put(watchId, listener);

        return cache.addListenerAsync(listener).thenApply(v -> watchId);
    }

    @Override
    public CompletableFuture<Void> unwatch(long watchId) {
        Object listener = activeWatchers.remove(watchId);
        if (listener != null) {
            return cache.removeListenerAsync(listener);
        }
        return CompletableFuture.completedFuture(null);
    }

    // --- Inner Implementations ---

    /**
     * Represents a Distributed Lock backed by an expiring cache entry.
     */
    private class InfinispanLock implements Lock {
        private final String lockKey;
        private final String lockOwnerId;
        private final long ttlSeconds;

        public InfinispanLock(String lockKey, String lockOwnerId, long ttlSeconds) {
            this.lockKey = lockKey;
            this.lockOwnerId = lockOwnerId;
            this.ttlSeconds = ttlSeconds;
        }

        @Override
        public CompletableFuture<Void> release() {
            // Atomic remove: only removes if the value matches our owner ID
            return cache.removeAsync(lockKey, lockOwnerId).thenApply(ignore -> null);
        }

        @Override
        public CompletableFuture<Void> refresh() {
            // Atomic replace: only refreshes TTL if we still hold the lock
            return cache.replaceAsync(lockKey, lockOwnerId, lockOwnerId, ttlSeconds, TimeUnit.SECONDS)
                    .thenApply(ignore -> null);
        }
    }

    /**
     * Clustered Listener to watch for distributed key changes.
     */
    @Listener(clustered = true, observation = Listener.Observation.POST)
    private class KeyEventListener {
        private final String targetKey;
        private final KeyWatcher watcher;

        public KeyEventListener(String targetKey, KeyWatcher watcher) {
            this.targetKey = targetKey;
            this.watcher = watcher;
        }

        @CacheEntryCreated
        public void onCreated(CacheEntryCreatedEvent<String, String> event) {
            if (event.isPre()) return;
            if (targetKey.equals(event.getKey())) {
                watcher.onPut(event.getKey(), event.getValue());
            }
        }

        @CacheEntryModified
        public void onModified(CacheEntryModifiedEvent<String, String> event) {
            if (event.isPre()) return;
            if (targetKey.equals(event.getKey())) {
                watcher.onPut(event.getKey(), event.getNewValue());
            }
        }

        @CacheEntryRemoved
        public void onRemoved(CacheEntryRemovedEvent<String, String> event) {
            if (event.isPre()) return;
            if (targetKey.equals(event.getKey())) {
                watcher.onDelete(event.getKey());
            }
        }
    }
}