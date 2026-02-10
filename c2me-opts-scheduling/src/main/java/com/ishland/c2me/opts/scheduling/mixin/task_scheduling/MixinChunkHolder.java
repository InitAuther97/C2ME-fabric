package com.ishland.c2me.opts.scheduling.mixin.task_scheduling;

import com.ishland.c2me.opts.scheduling.common.AtomicBitSet;
import com.ishland.c2me.opts.scheduling.common.BitSetUtil;
import com.ishland.c2me.opts.scheduling.common.DuckChunkHolder;
import it.unimi.dsi.fastutil.ints.IntIterator;
import net.minecraft.server.world.ChunkHolder;
import net.minecraft.util.math.ChunkPos;
import net.minecraft.world.HeightLimitView;
import net.minecraft.world.LightType;
import net.minecraft.world.chunk.light.LightingProvider;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.BitSet;

@Mixin(ChunkHolder.class)
public abstract class MixinChunkHolder implements DuckChunkHolder {

    @Shadow public abstract boolean markForLightUpdate(LightType lightType, int y);

    @Shadow @Final private LightingProvider lightingProvider;
    @Shadow
    @Final
    private BitSet skyLightUpdateBits;
    @Shadow
    @Final
    private BitSet blockLightUpdateBits;
    private AtomicBitSet[] c2me$dirtyLightSections;
    @SuppressWarnings("unused")
    private volatile boolean c2me$scheduledLightUndirty;

    @Inject(method = "<init>", at = @At("RETURN"))
    private void onInit(ChunkPos pos, int level, HeightLimitView world, LightingProvider lightingProvider, ChunkHolder.LevelUpdateListener levelUpdateListener, ChunkHolder.PlayersWatchingChunkProvider playersWatchingChunkProvider, CallbackInfo ci) {
        c2me$dirtyLightSections = new AtomicBitSet[LIGHT_TYPES.length];
        final int length = this.lightingProvider.getHeight() + 1;
        for (int i = 0; i < c2me$dirtyLightSections.length; i++) {
            c2me$dirtyLightSections[i] = AtomicBitSet.create(length);
        }
    }

    @Override
    public boolean c2me$queueLightSectionDirty(LightType lightType, int sectionY) {
        if (sectionY < this.lightingProvider.getBottomY() || sectionY > this.lightingProvider.getTopY()) return false;
        this.c2me$dirtyLightSections[lightType.ordinal()].set(sectionY - this.lightingProvider.getBottomY());
        // We need to guarantee that:
        // 1) if we see false, then we need to schedule, and the scheduled undirty
        // action will see our change. Therefore, release is needed.
        // 2) if we see true, then we don't need to schedule, and the undirty action
        // to come will see our change. Therefore, release is needed.

        // InitAuther97: These are signature polymorphic which references the Mixin class after compilation. Force cast to ChunkHolder to avoid such thing.
        // noinspection JavaLangInvokeHandleSignature
        return !(boolean) VH_LIGHT_UNDIRTY.getAndSetRelease((ChunkHolder)(Object) this, true);
    }

    @Override
    public boolean c2me$undirtyLight() {
        // InitAuther97: These are signature polymorphic which references the Mixin class after compilation. Force cast to ChunkHolder to avoid such thing.
        // noinspection JavaLangInvokeHandleSignature
        if (!(boolean) VH_LIGHT_UNDIRTY.getAndSetAcquire((ChunkHolder)(Object) this, false)) {
            // Synchronize with queueLightSectionDirty
            // This should probably never happen, but why not?
            // TODO: Add logging for indication that this branch is reached
            return false;
        }
        boolean hasDirtyLight = false;
        AtomicBitSet[] sections = this.c2me$dirtyLightSections;
        final int bottomY = this.lightingProvider.getBottomY();
        for (int i = 0; i < sections.length; i++) {
            LightType lightType = LIGHT_TYPES[i];
            switch (lightType) {
                case SKY -> hasDirtyLight |= BitSetUtil.setAll(this.skyLightUpdateBits, sections[i].getAllAndClear());
                case BLOCK -> hasDirtyLight |= BitSetUtil.setAll(this.blockLightUpdateBits, sections[i].getAllAndClear());
                case null, default -> {
                    // What if this is possible?
                    hasDirtyLight = false;
                    IntIterator section = sections[i].clearAndIterate();
                    while (section.hasNext()) {
                        int next = section.nextInt();
                        hasDirtyLight |= this.markForLightUpdate(lightType, next + bottomY);
                    }
                }
            }
        }
        return hasDirtyLight;
    }

    @Unique
    private static final VarHandle VH_LIGHT_UNDIRTY;
    @Unique
    private static final LightType[] LIGHT_TYPES = LightType.values();
    static {
        try {
            VH_LIGHT_UNDIRTY = MethodHandles.lookup().findVarHandle(MixinChunkHolder.class, "c2me$scheduledLightUndirty", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
