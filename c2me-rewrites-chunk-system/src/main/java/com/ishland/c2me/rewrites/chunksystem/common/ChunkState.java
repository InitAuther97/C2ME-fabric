package com.ishland.c2me.rewrites.chunksystem.common;

import net.minecraft.world.chunk.Chunk;
import net.minecraft.world.chunk.ChunkStatus;
import net.minecraft.world.chunk.ProtoChunk;

import java.lang.ScopedValue;

public record ChunkState(Chunk chunk, ProtoChunk protoChunk, ChunkStatus reachedStatus) {

    public static final ScopedValue<Boolean> UNLOADING = ScopedValue.newInstance();
}
