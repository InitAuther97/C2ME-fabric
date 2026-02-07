package com.ishland.c2me.rewrites.chunkio.common;

import com.google.common.base.Preconditions;
import com.ibm.asyncutil.util.Either;
import com.ishland.c2me.base.common.GlobalExecutors;
import com.ishland.c2me.base.common.theinterface.IDirectStorage;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import net.minecraft.nbt.NbtCompound;
import net.minecraft.nbt.scanner.NbtScanner;
import net.minecraft.util.math.ChunkPos;
import net.minecraft.world.storage.RegionBasedStorage;
import net.minecraft.world.storage.StorageIoWorker;
import net.minecraft.world.storage.StorageKey;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.LongFunction;
import java.util.function.Supplier;

public class C2MEStorageVanillaInterface extends StorageIoWorker implements IDirectStorage {

    private final C2MEStorageHandle backend;

    public C2MEStorageVanillaInterface(StorageKey arg, Path path, boolean dsync) {
        super(arg, path, dsync);
        this.backend = new C2MEStorageHandle(
                new RegionBasedStorage(arg, path, dsync),
                GlobalExecutors.prioritizedScheduler.executor(16)
        );
        StoragePool.runStorage(this.backend);
    }

    public C2MEStorageVanillaInterface(StorageKey arg, Path path, boolean dsync, LongFunction<Executor> prioritizedExecutor) {
        super(arg, path, dsync);
        this.backend = new C2MEStorageHandle(
                new RegionBasedStorage(arg, path, dsync),
                GlobalExecutors.prioritizedScheduler.executor(16),
                prioritizedExecutor
        );
        StoragePool.runStorage(this.backend);
    }

    @Override
    public CompletableFuture<Void> setResult(ChunkPos pos, @Nullable NbtCompound nbt) {
        return Completable.create(emitter -> {
            final var cache = new C2MEStorageHandle.DataCache(this.backend, Either.left(nbt));
            StorageRequest.WriteRequest request = new StorageRequest.WriteRequest(emitter, pos, cache);
            this.backend.enqueue(request);
        }).<Void>toCompletionStage(null).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> setResult(ChunkPos pos, Supplier<NbtCompound> nbtSupplier) {
        return Completable.create(emitter -> {
            final var cache = new C2MEStorageHandle.DataCache(this.backend);
            StorageRequest.WriteRequest request = new StorageRequest.WriteRequest(emitter, pos, cache);
            StoragePool.awaitVirtually(nbtSupplier).subscribe(cache);
            this.backend.enqueue(request);
        }).<Void>toCompletionStage(null).toCompletableFuture();
    }

    @Override
    public Completable setRawChunkData(ChunkPos pos, Single<Either<NbtCompound, byte[]>> data) {
        return Completable.create(emitter -> {
            final var cache = new C2MEStorageHandle.DataCache(this.backend);
            StorageRequest.WriteRequest request = new StorageRequest.WriteRequest(emitter, pos, cache);
            data.subscribe(cache);
            this.backend.enqueue(request);
        });
    }

    @Override
    public Completable setRawChunkData(ChunkPos pos, Either<NbtCompound, byte[]> data) {
        return Completable.create(emitter -> {
            final var cache = new C2MEStorageHandle.DataCache(this.backend, data);
            StorageRequest.WriteRequest request = new StorageRequest.WriteRequest(emitter, pos, cache);
            this.backend.enqueue(request);
        });
    }

    @Override
    public CompletableFuture<Optional<NbtCompound>> readChunkData(ChunkPos pos) {
        return Maybe.<NbtCompound>create(emitter -> this.backend.enqueue(new StorageRequest.ReadRequest(emitter, pos)))
                .map(Optional::of).toCompletionStage(Optional.empty()).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> completeAll(boolean sync) {
        return Completable.create(emitter -> {
            StorageRequest.FlushRequest request = new StorageRequest.FlushRequest(emitter, true); // Always sync
            this.backend.enqueue(request);
        }).<Void>toCompletionStage(null).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> scanChunk(ChunkPos pos, NbtScanner scanner) {
        Preconditions.checkNotNull(scanner, "scanner");
        return Completable.create(emitter -> {
            StorageRequest.ScanRequest request = new StorageRequest.ScanRequest(emitter, pos, scanner);
            this.backend.enqueue(request);
        }).<Void>toCompletionStage(null).toCompletableFuture();
    }

    @Override
    public void close() {
        Completable.create(emitter -> {
            StorageRequest.FlushRequest request = new StorageRequest.FlushRequest(emitter, true); // Always sync
            this.backend.enqueue(request);
        }).blockingSubscribe(this.backend::close, _ -> this.backend.close());
    }

    @Override
    public boolean needsBlending(ChunkPos chunkPos, int i) {
        return super.needsBlending(chunkPos, i);
    }

    @Override
    public StorageKey getStorageKey() {
        return this.backend.getStorageKey();
    }
}
