package com.ishland.c2me.rewrites.chunksystem.mixin;

import com.ishland.c2me.rewrites.chunksystem.common.ChunkState;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import net.minecraft.world.chunk.ChunkNibbleArray;
import net.minecraft.world.chunk.ChunkSection;
import net.minecraft.world.chunk.SerializedChunk;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;

@Mixin(SerializedChunk.class)
public class MixinSerializedChunk {
    @WrapOperation(method = "fromChunk", at = @At(value = "INVOKE", target = "Lnet/minecraft/world/chunk/ChunkNibbleArray;copy()Lnet/minecraft/world/chunk/ChunkNibbleArray;"))
    private static ChunkNibbleArray c2me$skipNibbleCopyOnUnload(ChunkNibbleArray instance, Operation<ChunkNibbleArray> original) {
        if (ChunkState.UNLOADING.isBound()) {
            return instance;
        }
        return original.call(instance);
    }

    @WrapOperation(method = "fromChunk", at = @At(value = "INVOKE", target = "Lnet/minecraft/world/chunk/ChunkSection;copy()Lnet/minecraft/world/chunk/ChunkSection;"))
    private static ChunkSection c2me$skipSectionCopyOnUnload(ChunkSection instance, Operation<ChunkSection> original) {
        if (ChunkState.UNLOADING.isBound()) {
            return instance;
        }
        return original.call(instance);
    }
}
