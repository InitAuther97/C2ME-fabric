package com.ishland.c2me.rewrites.chunkio.common;

import com.ibm.asyncutil.util.Either;
import com.ishland.c2me.base.common.TheSpeedyObjectFactory;
import com.ishland.c2me.base.mixin.access.IRegionBasedStorage;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.schedulers.Schedulers;
import it.unimi.dsi.fastutil.longs.Long2ReferenceLinkedOpenHashMap;
import net.minecraft.nbt.NbtCompound;
import net.minecraft.nbt.NbtIo;
import net.minecraft.nbt.NbtSizeTracker;
import net.minecraft.nbt.scanner.NbtScanner;
import net.minecraft.util.math.ChunkPos;
import net.minecraft.world.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.function.LongFunction;

public class C2MEStorageHandle implements Runnable {

    static final Logger LOGGER = LoggerFactory.getLogger("C2ME Storage");

    private final RegionBasedStorage storage;
    private final LongFunction<Scheduler> prioritizedScheduler;
    private final Scheduler ioScheduler;
    private final Long2ReferenceLinkedOpenHashMap<WriteCache> cache = new Long2ReferenceLinkedOpenHashMap<>();

    private final Semaphore sync = new Semaphore(0);
    private final Queue<Runnable> pendingTasks = new ConcurrentLinkedQueue<>();
    private final Executor executor = task -> {
        pendingTasks.offer(task);
        sync.release();
    };
    private final Scheduler storageScheduler = Schedulers.from(executor);

    private boolean closing = false;
    private CompletableEmitter flush;

    public C2MEStorageHandle(RegionBasedStorage storage, Scheduler backgroundScheduler) {
        this(storage, backgroundScheduler, unused -> backgroundScheduler);
    }

    public C2MEStorageHandle(RegionBasedStorage storage, Scheduler ioScheduler, LongFunction<Scheduler> prioritizedScheduler) {
        this.storage = storage;
        this.ioScheduler = ioScheduler;
        this.prioritizedScheduler = prioritizedScheduler;
    }

    @Override
    public String toString() {
        return String.format("C2MEStorageHandle[%s]", this.storage.getStorageKey());
    }

    @Override
    public void run() {
        while (!closing) {
            try {
                sync.acquire();
            } catch (InterruptedException e) {
                LOGGER.warn("Ignoring interruption while waiting for tasks", e);
                continue;
            }
            int count = 0;
            Runnable work;
            while ((work = pendingTasks.poll()) != null) {
                work.run();
                count++;
            }
            if (count == 0) {
                LOGGER.warn("Received redundant task permit");
                continue;
            } else if (count != 1) {
                sync.acquireUninterruptibly(count - 1);
            }
            final var flush = this.flush;
            if (flush != null && this.cache.isEmpty()) {
                flush.onComplete();
                this.flush = null;
            }
        }
        try {
            this.storage.close();
        } catch (Throwable t) {
            LOGGER.error("Error closing storage", t);
        }
    }

    private boolean hasPendingTasks() {
        return !this.pendingTasks.isEmpty() || !this.cache.isEmpty();
    }

    public StorageKey getStorageKey() {
        return this.storage.getStorageKey();
    }

    private Completable internalFlush(boolean sync) {
        final var flush = Completable
                .create(emitter -> this.flush = emitter)
                .subscribeOn(storageScheduler);
        if (sync) {
            return flush.doOnComplete(storage::sync);
        }
        return flush;
    }

    public CompletableFuture<Void> flush(boolean sync) {
        return internalFlush(sync)
                .<Void>toCompletionStage(null)
                .toCompletableFuture();
    }

    public void close() {
        internalFlush(true)
                .doOnComplete(() -> this.closing = true)
                .subscribe();
    }

    /**
     * Read chunk data from storage
     * @param pos target pos
     * @param scanner if null then ignored, if non-null then used and produce null future
     * @return future
     */
    public CompletableFuture<NbtCompound> getChunkData(long pos, NbtScanner scanner) {
        if (this.closing) {
            return CompletableFuture.failedFuture(new CancellationException());
        }
//        future.thenApply(Function.identity()).orTimeout(60, TimeUnit.SECONDS).exceptionally(throwable -> {
//            if (throwable instanceof TimeoutException) {
//                LOGGER.warn("Chunk read at pos {} took too long (> 1min)", new ChunkPos(pos).toLong());
//            }
//            return null;
//        });
        return scanner == null ?
                this.read0(pos).toCompletionStage(null).toCompletableFuture() :
                this.scan0(pos, scanner).<NbtCompound>toCompletionStage(null).toCompletableFuture();
    }

    private Maybe<Either<NbtCompound, byte[]>> readFromCache0(long pos) {
        return Single.just(this.cache)
                .observeOn(storageScheduler)
                .mapOptional(map -> Optional.ofNullable(map.get(pos)))
                .flatMap(WriteCache::get)
                .doOnError(unused -> {
                    LOGGER.warn("Need to retry read of chunk {} because previous write to chunk threw an exception", new ChunkPos(pos));
                    this.cache.remove(pos);
                })
                .onErrorComplete();
    }

    private Maybe<DataInputStream> readFromFile0(long pos) {
        return Maybe.fromCallable(() -> {
            final ChunkPos pos1 = new ChunkPos(pos);
            final RegionFile regionFile = ((IRegionBasedStorage) this.storage).invokeGetRegionFile(pos1);
            return regionFile.getChunkInputStream(pos1);
        });
    }

    private Completable scan0(long pos, NbtScanner scanner) {
        return readFromCache0(pos)
                .switchIfEmpty(Maybe.defer(() -> scheduleChunkScan(pos, scanner).toMaybe()))
                .flatMapCompletable(either ->
                        either.fold(
                                compound -> Completable.fromAction(() -> compound.accept(scanner)),
                                data -> Completable.fromAction(() -> {
                                    final DataInputStream input = new DataInputStream(new ByteArrayInputStream(data));
                                    NbtIo.scan(input, scanner, NbtSizeTracker.ofUnlimitedBytes());
                                })
                        ).subscribeOn(prioritizedScheduler.apply(pos)));
    }

    private Maybe<NbtCompound> read0(long pos) {
        return readFromCache0(pos)
                .flatMapSingle(either ->
                        either.fold(
                                Single::just,
                                data -> Single.fromCallable(() -> NbtIo.readCompound(new DataInputStream(new ByteArrayInputStream(data))))
                        ).subscribeOn(prioritizedScheduler.apply(pos)))
                .switchIfEmpty(Maybe.defer(() -> scheduleChunkRead(pos)));
    }

    private Maybe<NbtCompound> scheduleChunkRead(long pos) {
        return readFromFile0(pos)
                .flatMap(stream ->
                        Maybe.fromCallable(() -> {
                            try (DataInputStream stream1 = stream) {
                                return NbtIo.readCompound(stream1);
                            }
                        }).subscribeOn(prioritizedScheduler.apply(pos))
                );
    }

    private Completable scheduleChunkScan(long pos, NbtScanner scanner) {
        return readFromFile0(pos)
                .flatMapCompletable(stream ->
                        Completable.fromAction(() -> {
                            try (DataInputStream stream1 = stream) {
                                NbtIo.scan(stream1, scanner, NbtSizeTracker.ofUnlimitedBytes());
                            }
                        }).subscribeOn(prioritizedScheduler.apply(pos))
                );
    }

    public Completable scheduleSave(long pos, Maybe<Either<NbtCompound, byte[]>> nbt) {
        return Single.just(nbt)
                .observeOn(storageScheduler)
                .map(it -> {
                    final var newCache = new WriteCache(pos, it);
                    var oldCache = this.cache.put(pos, newCache);
                    if (oldCache != null) oldCache.cancel();
                    newCache.state = WriteCache.State.AWAIT_DATA;
                    return newCache;
                })
                .flatMapCompletable(it -> scheduleChunkWrite(pos, it))
                .doOnEvent(it -> {
                    if (it != WriteCache.OUTDATED) this.cache.remove(pos);
                });
    }

    private Completable scheduleChunkWrite(long pos, WriteCache cache) {
        return cache.get()
                .observeOn(storageScheduler)
                .switchIfEmpty(Completable.fromAction(() -> deleteChunk(pos, cache)).toMaybe())
                .flatMapSingle(it -> serializeChunk(pos, cache, it).observeOn(storageScheduler))
                .flatMapCompletable(dos -> saveChunk(cache, dos))
                .doOnError(t -> {
                    if (t != WriteCache.OUTDATED) {
                        cache.state = WriteCache.State.DEAD;
                        LOGGER.error("Error while saving chunk {} in {}", new ChunkPos(pos), this.getStorageKey(), t);
                    }
                });
    }

    private void deleteChunk(long pos, WriteCache cache) throws Exception {
        if (cache.isCancelled()) {
            throw WriteCache.OUTDATED;
        }

        cache.state = WriteCache.State.WRITING;
        final ChunkPos pos1 = new ChunkPos(pos);
        final RegionFile regionFile;
        try {
            regionFile = ((IRegionBasedStorage) this.storage).invokeGetRegionFile(pos1);
        } catch (IOException e) {
            LOGGER.warn("Failed to get region file for chunk {}", pos1, e);
            throw e;
        }

        try {
            regionFile.delete(pos1);
        } catch (IOException e) {
            LOGGER.warn("Failed to remove existing data for chunk {}", pos1, e);
            throw e;
        }
    }

    private Single<DataOutputStream> serializeChunk(long pos, WriteCache cache, Either<NbtCompound, byte[]> either) {
        if (cache.isCancelled()) {
            return Single.error(WriteCache.OUTDATED);
        }
        cache.state = WriteCache.State.AWAIT_WRITE;
        final ChunkPos pos1 = new ChunkPos(pos);
        final RegionFile regionFile;
        final DataOutputStream dos;
        try {
            regionFile = ((IRegionBasedStorage) this.storage).invokeGetRegionFile(pos1);
            dos = regionFile.getChunkOutputStream(pos1);
        } catch (IOException e) {
            LOGGER.warn("Failed to write chunk data for chunk {} in {}", pos1, this.storage.getStorageKey(), e);
            return Single.error(e);
        }

        return Single.just(either)
                .observeOn(ioScheduler)
                .flatMapCompletable(it -> it.fold(
                        compound -> Completable.fromAction(() -> NbtIo.writeCompound(compound, dos)),
                        data -> Completable.fromAction(() -> dos.write(data))
                ))
                .toSingleDefault(dos);
    }

    private Completable saveChunk(WriteCache cache, DataOutputStream dos) {
        if (cache.isCancelled()) {
            return Completable.error(WriteCache.OUTDATED);
        }
        cache.state = WriteCache.State.WRITING;
        return Completable.fromAction(dos::close);
    }

}
