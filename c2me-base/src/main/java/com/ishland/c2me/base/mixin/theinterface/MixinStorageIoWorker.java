package com.ishland.c2me.base.mixin.theinterface;

import com.ibm.asyncutil.util.Either;
import com.ishland.c2me.base.common.theinterface.IDirectStorage;
import com.ishland.c2me.base.mixin.access.IRegionBasedStorage;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import net.minecraft.nbt.NbtCompound;
import net.minecraft.util.math.ChunkPos;
import net.minecraft.world.storage.RegionBasedStorage;
import net.minecraft.world.storage.RegionFile;
import net.minecraft.world.storage.StorageIoWorker;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.SequencedMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

@Mixin(StorageIoWorker.class)
public abstract class MixinStorageIoWorker implements IDirectStorage {

    @Shadow protected abstract <T> CompletableFuture<T> run(Supplier<T> task);

    @Shadow @Final private SequencedMap<ChunkPos, StorageIoWorker.Result> results;

    @Shadow @Final private RegionBasedStorage storage;

    @Unique
    private CompletableFuture<?> c2me$setRawChunkData0(ChunkPos pos, Either<NbtCompound, byte[]> data) {
        StorageIoWorker.Result result = this.results.get(pos);
        if (data.isLeft()) {
            NbtCompound nbtCompound = data.left().get();
            if (result == null) {
                final var newResult = new StorageIoWorker.Result(nbtCompound);
                this.results.put(pos, newResult);
                return newResult.future;
            } else {
                result.nbt = nbtCompound;
                return result.future;
            }
        } else {
            try {
                final RegionFile regionFile = ((IRegionBasedStorage) this.storage).invokeGetRegionFile(pos);
                try (final DataOutputStream out = regionFile.getChunkOutputStream(pos)) {
                    out.write(data.right().get());
                }
                if (result != null) {
                    result.future.complete(null);
                }
                return CompletableFuture.completedFuture(null);
            } catch (IOException e) {
                // Do not fail future for result's future
                return CompletableFuture.failedFuture(e);
            }
        }
    }

    @Override
    public Completable setRawChunkData(ChunkPos pos, Single<Either<NbtCompound, byte[]>> single) {
        return Completable.fromCompletionStage(this.run(() -> this.c2me$setRawChunkData0(pos, single.blockingGet())).thenCompose(Function.identity()));
    }
}
