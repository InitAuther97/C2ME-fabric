package com.ishland.c2me.base.common.theinterface;

import com.ibm.asyncutil.util.Either;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import net.minecraft.nbt.NbtCompound;
import net.minecraft.util.math.ChunkPos;

public interface IDirectStorage {

    Completable setRawChunkData(ChunkPos pos, Single<Either<NbtCompound, byte[]>> data);
    Completable setRawChunkData(ChunkPos pos, Either<NbtCompound, byte[]> data);
}
