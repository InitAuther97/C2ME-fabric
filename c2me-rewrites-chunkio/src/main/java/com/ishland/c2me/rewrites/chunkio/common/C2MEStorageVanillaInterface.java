package com.ishland.c2me.rewrites.chunkio.common;

import com.google.common.base.Preconditions;
import com.ibm.asyncutil.util.Either;
import com.ishland.c2me.base.common.GlobalExecutors;
import com.ishland.c2me.base.common.theinterface.IDirectStorage;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.schedulers.Schedulers;
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
    private final CompletableFuture<?> backendFuture;

    public C2MEStorageVanillaInterface(StorageKey arg, Path path, boolean dsync) {
        super(arg, path, dsync);
        this.backend = new C2MEStorageHandle(
                new RegionBasedStorage(arg, path, dsync),
                Schedulers.from(GlobalExecutors.prioritizedScheduler.executor(16))
        );
        this.backendFuture = StoragePool.runStorage(this.backend);
    }

    public C2MEStorageVanillaInterface(StorageKey arg, Path path, boolean dsync, LongFunction<Executor> prioritizedExecutor) {
        super(arg, path, dsync);
        this.backend = new C2MEStorageHandle(
                new RegionBasedStorage(arg, path, dsync),
                Schedulers.from(GlobalExecutors.prioritizedScheduler.executor(16)),
                pos -> Schedulers.from(prioritizedExecutor.apply(pos))
        );
        this.backendFuture = StoragePool.runStorage(this.backend);
    }

    @Override
    public CompletableFuture<Void> setResult(ChunkPos pos, @Nullable NbtCompound nbt) {
        return this.backend.scheduleSave(
                pos.toLong(),
                nbt == null ? Maybe.empty() : Maybe.just(Either.left(nbt))
        ).onErrorComplete(WriteCache.OUTDATED::equals).<Void>toCompletionStage(null).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> setResult(ChunkPos pos, Supplier<NbtCompound> nbtSupplier) {
        return this.backend.scheduleSave(
                pos.toLong(),
                StoragePool.awaitVirtually(nbtSupplier) // nonblocking write
        ).onErrorComplete(WriteCache.OUTDATED::equals).<Void>toCompletionStage(null).toCompletableFuture();
    }

    @Override
    public Completable setRawChunkData(ChunkPos pos, Either<NbtCompound, byte[]> data) {
        return this.backend.scheduleSave(
                pos.toLong(),
                Maybe.just(data)
        ).onErrorComplete(WriteCache.OUTDATED::equals);
    }

    @Override
    public CompletableFuture<Optional<NbtCompound>> readChunkData(ChunkPos pos) {
        return this.backend.getChunkData(pos.toLong(), null).thenApply(Optional::ofNullable);
    }

    @Override
    public CompletableFuture<Void> completeAll(boolean sync) {
        return this.backend.flush(true).copy();
    }

    @Override
    public CompletableFuture<Void> scanChunk(ChunkPos pos, NbtScanner scanner) {
        Preconditions.checkNotNull(scanner, "scanner");
        return this.backend.getChunkData(pos.toLong(), scanner).thenRun(() -> {});
    }

    @Override
    public void close() {
        this.backend.close();
        this.backendFuture.join();
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
