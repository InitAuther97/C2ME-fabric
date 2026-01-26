package com.ishland.c2me.base.mixin.theinterface;

import com.ibm.asyncutil.util.Either;
import com.ishland.c2me.base.common.theinterface.IDirectStorage;
import com.ishland.c2me.base.mixin.access.IRegionBasedStorage;
import io.reactivex.rxjava3.core.Completable;
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
import java.util.function.Supplier;

@Mixin(StorageIoWorker.class)
public abstract class MixinStorageIoWorker implements IDirectStorage {

    @Shadow protected abstract <T> CompletableFuture<T> run(Supplier<T> task);

    @Shadow @Final private SequencedMap<ChunkPos, StorageIoWorker.Result> results;

    @Shadow @Final private RegionBasedStorage storage;

    @Shadow
    public abstract CompletableFuture<Void> setResult(ChunkPos pos, Supplier<NbtCompound> nbtSupplier);

    @Unique
    private void c2me$setRawChunkData0(ChunkPos pos, byte[] data) throws IOException {
        StorageIoWorker.Result result = this.results.get(pos);
        final RegionFile regionFile = ((IRegionBasedStorage) this.storage).invokeGetRegionFile(pos);
        try (final DataOutputStream out = regionFile.getChunkOutputStream(pos)) {
            out.write(data);
        }
        if (result != null) {
            result.future.complete(null);
        }
    }

    @Override
    public Completable setRawChunkData(ChunkPos pos, Either<NbtCompound, byte[]> either) {
        return either.fold(
                compound -> Completable.fromCompletionStage(this.setResult(pos, () -> compound)),
                data -> Completable.fromAction(() -> c2me$setRawChunkData0(pos, data))
        );
    }
}
