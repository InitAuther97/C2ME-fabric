package com.ishland.c2me.opts.scheduling.common;

import net.minecraft.world.LightType;

public interface DuckChunkHolder {

    boolean c2me$queueLightSectionDirty(LightType lightType, int sectionY);

    boolean c2me$undirtyLight();
}
