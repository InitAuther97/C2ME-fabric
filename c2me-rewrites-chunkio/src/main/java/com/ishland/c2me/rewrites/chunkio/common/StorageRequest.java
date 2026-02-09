package com.ishland.c2me.rewrites.chunkio.common;

import com.google.common.base.Preconditions;
import com.ibm.asyncutil.util.Either;
import com.ishland.c2me.base.common.structs.RawByteArrayOutputStream;
import com.ishland.c2me.base.mixin.access.IRegionBasedStorage;
import com.ishland.c2me.base.mixin.access.IRegionFile;
import io.reactivex.rxjava3.core.CompletableEmitter;
import io.reactivex.rxjava3.core.MaybeEmitter;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import net.minecraft.nbt.NbtCompound;
import net.minecraft.nbt.NbtIo;
import net.minecraft.nbt.NbtSizeTracker;
import net.minecraft.nbt.scanner.NbtScanner;
import net.minecraft.util.math.ChunkPos;
import net.minecraft.world.storage.ChunkCompressionFormat;
import net.minecraft.world.storage.RegionFile;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Function;

public sealed interface StorageRequest {

    void accept(C2MEStorageHandle worker);

    abstract sealed class ResultStorageRequest<T> implements StorageRequest {
        protected final MaybeEmitter<T> callback;

        public ResultStorageRequest(MaybeEmitter<T> emitter) {
            this.callback = emitter;
        }
    }

    abstract sealed class CompletableStorageRequest implements StorageRequest {
        protected final CompletableEmitter callback;

        public CompletableStorageRequest(CompletableEmitter callback) {
            this.callback = callback;
        }
    }

    final class ReadRequest extends ResultStorageRequest<NbtCompound> implements Runnable {

        final ChunkPos pos;
        C2MEStorageHandle.DataCache cache;
        DataInputStream stream;

        public ReadRequest(MaybeEmitter<NbtCompound> emitter, ChunkPos pos) {
            super(emitter);
            this.pos = pos;
        }

        @Override
        public void accept(C2MEStorageHandle worker) {
            final var pos = this.pos;
            final var cache = this.cache;
            final Either<Optional<Either<NbtCompound, byte[]>>, Throwable> result;
            if (cache == null) {
                // We try to wait for cache to complete; if we need to wait, we'll quit now.
                // Write to cache is guaranteed to be visible when it runs for the next time;
                // Though, it is already guaranteed, because it happens on the same thread.
                // This may be broken when we switch between threads. In this case queueOrGet
                // ensures that it's correct.
                final var cacheRead = worker.getCache(pos.toLong());
                if (cacheRead == null) {
                    result = null;
                } else if ((result = cacheRead.queueOrGet(this)) == null) {
                    this.cache = cacheRead;
                    return;
                }
            } else {
                result = cache.getNow();
                this.cache = null; // Release data cache
            }
            final Either<NbtCompound, byte[]> dataNow;
            if (result == null) {
                dataNow = null;
            } else if (result.isLeft()) {
                final var optional = result.left().get();
                if (optional.isEmpty()) {
                    callback.onComplete();
                    return;
                }
                dataNow = optional.get();
            } else {
                final var throwable = result.right().get();
                C2MEStorageHandle.LOGGER.warn("Need to retry read of chunk {} because previous write throws an exception", pos, throwable);
                dataNow = null;
            }
            if (dataNow == null) {
                try {
                    final RegionFile regionFile = ((IRegionBasedStorage) worker.accessStorage()).invokeGetRegionFile(pos);
                    final var stream = regionFile.getChunkInputStream(pos);
                    if (stream == null) {
                        callback.onComplete();
                        return;
                    }
                    this.stream = stream;
                } catch (IOException e) {
                    callback.tryOnError(e);
                    return;
                }
            } else if (dataNow.isLeft()) {
                callback.onSuccess(dataNow.left().get());
                return;
            } else {
                this.stream = new DataInputStream(new ByteArrayInputStream(dataNow.right().get()));
            }
            worker.prioritized(pos.toLong()).execute(this);
        }

        @Override
        public void run() {
            try (var in = stream) {
                callback.onSuccess(NbtIo.readCompound(in));
            } catch (IOException e) {
                callback.tryOnError(e);
            }
        }
    }

    final class ScanRequest extends CompletableStorageRequest implements Runnable {

        private static final Function<NbtCompound, NbtCompound> IDENTITY = Function.identity();
        private static final Function<byte[], DataInputStream> WRAP = ((Function<byte[], ByteArrayInputStream>) ByteArrayInputStream::new).andThen(DataInputStream::new);

        private final ChunkPos pos;
        private final NbtScanner scanner;
        C2MEStorageHandle.DataCache cache;
        private Either<NbtCompound, DataInputStream> data;

        public ScanRequest(CompletableEmitter emitter, ChunkPos pos, NbtScanner scanner) {
            super(emitter);
            this.pos = pos;
            this.scanner = scanner;
        }

        @Override
        public void accept(C2MEStorageHandle worker) {
            final var pos = this.pos;
            final var cache = this.cache;
            final Either<Optional<Either<NbtCompound, byte[]>>, Throwable> result;
            if (cache == null) {
                // We try to wait for cache to complete; if we need to wait, we'll quit now.
                // Write to cache is guaranteed to be visible when it runs for the next time;
                // Though, it is already guaranteed, because it happens on the same thread.
                // This may be broken when we switch between threads. In this case queueOrGet
                // ensures that it's correct.
                final var cacheRead = worker.getCache(pos.toLong());
                if (cacheRead == null) {
                    result = null;
                } else if ((result = cacheRead.queueOrGet(this)) == null) {
                    this.cache = cacheRead;
                    return;
                }
            } else {
                result = cache.getNow();
                this.cache = null; // Release data cache
            }
            final Either<NbtCompound, byte[]> dataNow;
            if (result == null) {
                dataNow = null;
            } else if (result.isLeft()) {
                final var optional = result.left().get();
                if (optional.isEmpty()) {
                    callback.onComplete();
                    return;
                }
                dataNow = optional.get();
            } else {
                final var throwable = result.right().get();
                C2MEStorageHandle.LOGGER.warn("Need to retry read of chunk {} because previous write throws an exception", pos, throwable);
                dataNow = null;
            }
            if (dataNow == null) {
                try {
                    final RegionFile regionFile = ((IRegionBasedStorage) worker.accessStorage()).invokeGetRegionFile(pos);
                    final var stream = regionFile.getChunkInputStream(pos);
                    if (stream == null) {
                        callback.onComplete();
                        return;
                    }
                    this.data = Either.right(stream);
                } catch (IOException e) {
                    callback.tryOnError(e);
                    return;
                }
            } else {
                this.data = dataNow.map(IDENTITY, WRAP);
            }
            worker.prioritized(pos.toLong()).execute(this);
        }

        @Override
        public void run() {
            if (data.isLeft()) {
                data.left().get().accept(scanner);
            } else {
                try (final var in = data.right().get()) {
                    NbtIo.scan(in, scanner, NbtSizeTracker.ofUnlimitedBytes());
                } catch (IOException e) {
                    callback.tryOnError(e);
                    return;
                }
            }
            callback.onComplete();
        }
    }

    final class WriteRequest extends CompletableStorageRequest implements Runnable {

        private final ChunkPos pos;
        private final C2MEStorageHandle.DataCache cache;
        private Either<NbtCompound, byte[]> data;
        private DataOutputStream dos;
        private RawByteArrayOutputStream baos;
        private byte state = 0;

        public WriteRequest(CompletableEmitter emitter, ChunkPos pos, C2MEStorageHandle.DataCache cache) {
            super(emitter);
            Preconditions.checkArgument(cache != null, "cache cannot be null");
            this.pos = pos;
            this.cache = cache;
        }

        @Override
        public void accept(C2MEStorageHandle worker) {
            final var cache = this.cache;
            final var pos = this.pos;
            final Either<Optional<Either<NbtCompound, byte[]>>, Throwable> result;
            final byte state = this.state;
            if (state == 0) {
                worker.setCache(pos.toLong(), cache);
                callback.onComplete();
                this.state = 1;
                // We try to wait for cache to complete; if we need to wait, we'll quit now.
                // Write to cache is guaranteed to be visible when it runs for the next time;
                // Though, it is already guaranteed, because it happens on the same thread.
                // This may be broken when we switch between threads. In this case queueOrGet
                // ensures that it's correct.
                result = cache.queueOrGet(this);
                if (result == null) {
                    return;
                }
            } else if (state == 1) {
                if (!cache.isValid()) return;
                result = cache.getNow();
            } else if (state == 3) {
                if (!cache.isValid()) {
                    return;
                }
                try {
                    final RegionFile regionFile = ((IRegionBasedStorage) worker.accessStorage()).invokeGetRegionFile(pos);
                    ByteBuffer byteBuffer = baos.asByteBuffer();
                    // C2MEStorageHandle.LOGGER.info("buffer size is {}", byteBuffer.limit());
                    // TODO [VanillaCopy] RegionFile.ChunkBuffer
                    byteBuffer.putInt(0, baos.size() - 5 + 1);
                    ((IRegionFile) regionFile).invokeWriteChunk(pos, byteBuffer);
                } catch (IOException e) {
                    final var storage = worker.accessStorage();
                    C2MEStorageHandle.LOGGER.warn("Failed to write chunk data for chunk {} in {}", pos, storage.getStorageKey(), e);
                }
                worker.invalidateCache(pos.toLong());
                return;
            } else {
                throw new IllegalStateException("Unknown state for WriteRequest when scheduled to storage: " + this.state);
            }
            this.state = 2;
            if (result.isLeft()) {
                final var optional = result.left().get();
                if (optional.isEmpty()) {
                    final RegionFile regionFile;
                    try {
                        regionFile = ((IRegionBasedStorage) worker.accessStorage()).invokeGetRegionFile(pos);
                        regionFile.delete(pos);
                        worker.invalidateCache(pos.toLong());
                    } catch (IOException e) {
                        C2MEStorageHandle.LOGGER.warn("Failed to remove existing data for chunk {}", pos, e);
                    }
                    return;
                }
                this.data = optional.get();
            } else {
                final var throwable = result.right().get();
                C2MEStorageHandle.LOGGER.warn("Cannot save chunk {} because the data future failed with an exception", pos, throwable);
                return;
            }
            final RegionFile regionFile;
            final var storage = worker.accessStorage();
            ChunkCompressionFormat compressionFormat;
            {
                try {
                    regionFile = ((IRegionBasedStorage) storage).invokeGetRegionFile(pos);
                    compressionFormat = ((IRegionFile) regionFile).getCompressionFormat();
                } catch (Throwable t) {
                    C2MEStorageHandle.LOGGER.warn("Failed to get compression format for chunk {} in {}", pos, worker, t);
                    compressionFormat = ChunkCompressionFormat.getCurrentFormat();
                }
            }
            try {
                final RawByteArrayOutputStream out = new RawByteArrayOutputStream(8096);
                // TODO [VanillaCopy] RegionFile.ChunkBuffer
                out.write(0);
                out.write(0);
                out.write(0);
                out.write(0);
                out.write(compressionFormat.getId());
                this.baos = out;
                this.dos = new DataOutputStream(compressionFormat.wrap(out));
            } catch (Throwable t) {
                C2MEStorageHandle.LOGGER.warn("Failed to wrap compression stream for chunk {} in {}", pos, worker, t);
                return;
            }
            worker.io().execute(this);
        }

        @Override
        public void run() {
            if (!cache.isValid()) {
                // Relaxed read: if it's false then it's cancelled.
                // Otherwise, we can always observe the correct validness on storage thread
                return;
            }
            final var data = this.data;
            try {
                if (data.isLeft()) {
                    NbtIo.writeCompound(data.left().get(), this.dos);
                } else {
                    this.dos.write(data.right().get());
                }
                this.dos.close();
            } catch (IOException e) {
                C2MEStorageHandle.LOGGER.error("Failed to write chunk data for chunk {} in {}", pos, cache.handle.accessStorage().getStorageKey(), e);
            }
            this.state = 3;
            // Piggyback on schedule operation
            cache.handle.enqueue(this);
        }
    }

    final class FlushRequest extends CompletableStorageRequest {

        final LongOpenHashSet poses = new LongOpenHashSet();
        final boolean sync;

        public FlushRequest(CompletableEmitter callback, boolean sync) {
            super(callback);
            this.sync = sync;
        }

        @Override
        public void accept(C2MEStorageHandle worker) {
            worker.processFlushRequest(this);
        }
    }
}
