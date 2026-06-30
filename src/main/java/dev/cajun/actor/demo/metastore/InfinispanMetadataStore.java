package dev.cajun.actor.demo.metastore;

import com.cajunsystems.cluster.MetadataStore;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Infinispan embedded implementation of the MetadataStore.
 */
public class InfinispanMetadataStore implements MetadataStore {

    private static final Logger logger = LoggerFactory.getLogger(InfinispanMetadataStore.class);

    public static final String CACHE_NAME = "cajun-store";
    public static final String LEADER_KEY = "cajun/leader";

    private final EmbeddedCacheManager cacheManager;
    private final String nodeId;
    private Cache<String, String> cache;

    private final AtomicLong watcherIdGenerator = new AtomicLong(0);
    private final Map<Long, Object> activeWatchers = new ConcurrentHashMap<>();

    public InfinispanMetadataStore(EmbeddedCacheManager cacheManager, String nodeId) {
        this.cacheManager = cacheManager;
        this.nodeId = nodeId;
    }

    @Override
    public CompletableFuture<Void> connect() {
        return CompletableFuture.runAsync(() -> {
            this.cache = cacheManager.getCache(CACHE_NAME);
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

        // Using putIfAbsent with lifespan ensures atomic locking with a strict TTL
		// TODO: We have to increase ttlSeconds, because its refresh cycle is the same as the TTL.
        long extendedTtlSeconds = ttlSeconds + 2;

        return cache.putIfAbsentAsync(lockName, nodeId, extendedTtlSeconds, TimeUnit.SECONDS)
                .thenApply(existingValue -> {
                    if (existingValue == null) {
                        // Lock successfully acquired
                        return Optional.of(new InfinispanLock(lockName, nodeId, ttlSeconds));
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

        return cache.addListenerAsync(listener).thenApply(v -> watchId).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> unwatch(long watchId) {
        Object listener = activeWatchers.remove(watchId);
        if (listener != null) {
            return cache.removeListenerAsync(listener).toCompletableFuture();
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
            // TODO: We have to increase ttlSeconds, because its refresh cycle is the same as the TTL.
            long extendedTtlSeconds = ttlSeconds + 2;
            return cache.replaceAsync(lockKey, lockOwnerId, lockOwnerId, extendedTtlSeconds, TimeUnit.SECONDS)
                    .thenApply(replaced -> {
                        if (!replaced)
                            throw new RuntimeException("The lock is no longer in my possession.");
                        return null;
                    });
        }
    }

    /**
     * Clustered Listener to watch for distributed key changes.
     */
    @Listener(clustered = true, observation = Listener.Observation.POST)
    private record KeyEventListener(String targetKey, KeyWatcher watcher) {

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

        @CacheEntryExpired
        public void onExpiration(CacheEntryExpiredEvent<String, String> event) {
        }
    }
}