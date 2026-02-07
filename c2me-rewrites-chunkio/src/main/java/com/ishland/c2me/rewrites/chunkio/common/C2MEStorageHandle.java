package com.ishland.c2me.rewrites.chunkio.common;

import com.google.common.base.Preconditions;
import com.ibm.asyncutil.util.Either;
import com.ishland.c2me.base.common.TheSpeedyObjectFactory;
import com.ishland.flowsched.util.Assertions;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.disposables.Disposable;
import it.unimi.dsi.fastutil.longs.Long2ReferenceLinkedOpenHashMap;
import net.minecraft.nbt.NbtCompound;
import net.minecraft.world.storage.*;
import org.jctools.queues.MessagePassingQueue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongFunction;

public class C2MEStorageHandle implements Runnable, MessagePassingQueue.Consumer<StorageRequest>, AutoCloseable {

    static final Logger LOGGER = LoggerFactory.getLogger("C2ME Storage");

    private final RegionBasedStorage storage;
    private final Long2ReferenceLinkedOpenHashMap<DataCache> cache = new Long2ReferenceLinkedOpenHashMap<>();

    private final LongFunction<Executor> prioritizedExecutor;
    private final Executor ioExecutor;

    private Thread carrier;
    private final MessagePassingQueue<StorageRequest> pendingTasks;
    //private final boolean logThroughput;

    // Cold fields that are only changed by worker thread
    private boolean closing = false;
    List<StorageRequest.FlushRequest> flushRequests = new ArrayList<>(1);

    // Pre padding for hot field
    @SuppressWarnings("unused")
    private final int i0 = 0, i1 = 0, i2 = 0, i3 = 0,
            i4 = 0, i5 = 0, i6 = 0, i7 = 0,
            i8 = 0, i9 = 0, iA = 0, iB = 0,
            iC = 0, iD = 0, iE = 0, iF = 0;

    @SuppressWarnings("unused")
    private volatile int taskCount;

    // Post padding for hot field
    @SuppressWarnings("unused")
    private final int i10 = 0, i11 = 0, i12 = 0, i13 = 0,
            i14 = 0, i15 = 0, i16 = 0, i17 = 0,
            i18 = 0, i19 = 0, i1A = 0, i1B = 0,
            i1C = 0, i1D = 0, i1E = 0, i1F = 0;

    public C2MEStorageHandle(RegionBasedStorage storage, Executor backgroundExecutor) {
        this(storage, backgroundExecutor, _ -> backgroundExecutor);
    }

    public C2MEStorageHandle(RegionBasedStorage storage, Executor ioExecutor, LongFunction<Executor> prioritizedExecutor) {
        final var queue = TheSpeedyObjectFactory.INSTANCE.<StorageRequest>newMPSCQueue();
        Preconditions.checkArgument(queue instanceof MessagePassingQueue<?>, "MPSC queue is not MessagePassingQueue");
        this.pendingTasks = (MessagePassingQueue<StorageRequest>) queue;
        this.storage = storage;
        //if (storage.getStorageKey().type().equals("chunk")) {
        //    logThroughput = true;
        //} else logThroughput = false;
        this.ioExecutor = ioExecutor;
        this.prioritizedExecutor = prioritizedExecutor;
    }

    @Override
    public String toString() {
        return String.format("C2MEStorageHandle[%s]", this.storage.getStorageKey());
    }

    public void initCarrier() {
        Assertions.assertTrue(this.carrier == null, "Carrier already initialized");
        this.carrier = Thread.currentThread();
    }

    @Override
    public void run() {
        try {
            //int total = 0;
            while (!closing) {
                if (0 >= (int) VH_COUNT.getVolatile(this)) {
                    //if (logThroughput) LOGGER.info("Storage {} drain {}, cache size {}", this.storage.getStorageKey().dimension().getValue(), total, this.cache.size());
                    //total = 0;
                    LockSupport.park(this);
                    if (Thread.interrupted()) {
                        LOGGER.warn("Ignored interruption when waiting for tasks");
                    }
                }
                final int count = pendingTasks.drain(this);
                if (count == 0) continue;
                //total += count;
                VH_COUNT.getAndAddRelease(this, -count);
            }
            LOGGER.info("Storage {} finished execution", this);
        } catch (Throwable t) {
            LOGGER.error("Storage {} crashed", this, t);
            for (StorageRequest.FlushRequest flushRequest : flushRequests) {
                flushRequest.callback.tryOnError(t);
            }
        }
    }

    public void enqueue(@NotNull StorageRequest pending) {
        pendingTasks.offer(pending);
        final int count = (int) VH_COUNT.getAndAddAcquire(this, 1);
        if (count == 0) {
            LockSupport.unpark(carrier);
        }
    }

    @Override
    public void accept(StorageRequest e) {
        e.accept(this);
    }

    public StorageKey getStorageKey() {
        return this.storage.getStorageKey();
    }

    @Override
    public void close() {
        this.closing = true;
    }

    public Executor io() {
        return ioExecutor;
    }

    public Executor prioritized(long pos) {
        return prioritizedExecutor.apply(pos);
    }

    public void processFlushRequest(StorageRequest.FlushRequest request) {
        if (this.cache.isEmpty()) {
            request.callback.onComplete();
            return;
        }
        request.poses.addAll(this.cache.keySet());
        this.flushRequests.add(request);
    }

    public DataCache getCache(long pos) {
        return this.cache.get(pos);
    }

    public void setCache(long pos, DataCache data) {
        var oldCache = this.cache.put(pos, data);
        if (oldCache == null) {
            return;
        }
        oldCache.invalidate();
        onCacheCompletion(pos);
    }

    public void invalidateCache(long pos) {
        final var oldCache = this.cache.remove(pos);
        if (oldCache == null) {
            return;
        }
        oldCache.invalidate();
        onCacheCompletion(pos);
    }

    private void onCacheCompletion(long pos) {
        final var iterator = flushRequests.iterator();
        while (iterator.hasNext()) {
            final var request = iterator.next();
            request.poses.remove(pos);
            if (!request.poses.isEmpty()) {
                continue;
            }
            iterator.remove();
            if (request.sync) {
                try {
                    this.storage.sync();
                } catch (IOException e) {
                    LOGGER.error("Failed to synchronize chunks", e);
                    request.callback.tryOnError(e);
                }
            }
            request.callback.onComplete();
        }
    }

    public RegionBasedStorage accessStorage() {
        return this.storage;
    }

    private static final VarHandle VH_COUNT;

    static {
        try {
            VH_COUNT = MethodHandles.lookup().findVarHandle(C2MEStorageHandle.class, "taskCount", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static class DataCache implements MaybeObserver<Either<NbtCompound, byte[]>>, SingleObserver<Either<NbtCompound, byte[]>> {

        private static final StorageRequest[] DISPOSED = new StorageRequest[0];

        private Either<Optional<Either<NbtCompound, byte[]>>, Throwable> data;
        private volatile StorageRequest[] pending;
        boolean valid = true;
        final C2MEStorageHandle handle;

        public DataCache(C2MEStorageHandle handle) {
            this.handle = handle;
            this.pending = new StorageRequest[0];
        }

        public DataCache(C2MEStorageHandle handle, Either<NbtCompound, byte[]> data) {
            this.handle = handle;
            this.pending = DISPOSED;
            this.data = Either.left(Optional.ofNullable(data));
        }

        public void invalidate() {
            valid = false;
        }

        public boolean isValid() {
            return valid; // This could be lazy because we don't set it to INVALIDATED async
        }

        @Override
        public void onSubscribe(@NonNull Disposable d) {
            // We never cancel
        }

        @Override
        public void onSuccess(@NonNull Either<NbtCompound, byte[]> either) {
            this.data = Either.left(Optional.of(either)); // Piggyback on pending write
            onPublish();
        }

        @Override
        public void onError(@NonNull Throwable e) {
            this.data = Either.right(e);
            onPublish();
        }

        @Override
        public void onComplete() {
            this.data = Either.left(Optional.empty());
            onPublish();
        }

        private void onPublish() {
            final var pending = (StorageRequest[]) VH_PENDING.getAndSetRelease(this, DISPOSED);
            VarHandle.acquireFence();
            Assertions.assertTrue(DISPOSED != pending, "Racing condition: multiple completion");
            for (StorageRequest request : pending) {
                handle.enqueue(request);
            }
        }

        public Either<Optional<Either<NbtCompound, byte[]>>, Throwable> queueOrGet(StorageRequest request) {
            final var provided = VH_PENDING.getAcquire(this);
            if (provided == DISPOSED) {
                // Piggyback on pending read
                final var result = this.data;
                Assertions.assertTrue(result != null, "Racing condition: data read is null after pending disposed");
                return result;
            }
            final var appended = new StorageRequest[pending.length + 1];
            System.arraycopy(pending, 0, appended, 0, pending.length);
            appended[pending.length] = request;
            if (DISPOSED != VH_PENDING.compareAndExchangeRelease(this, provided, appended)) {
                return null;
            }
            VarHandle.acquireFence();
            // Piggyback on pending read
            final var result = this.data;
            Assertions.assertTrue(result != null, "Racing condition: data read is null after pending disposed");
            return result;
        }

        public Either<Optional<Either<NbtCompound, byte[]>>, Throwable> getNow() {
            return data;
        }

        private static final VarHandle VH_PENDING;

        static {
            try {
                VH_PENDING = MethodHandles.lookup().findVarHandle(DataCache.class, "pending", StorageRequest[].class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
