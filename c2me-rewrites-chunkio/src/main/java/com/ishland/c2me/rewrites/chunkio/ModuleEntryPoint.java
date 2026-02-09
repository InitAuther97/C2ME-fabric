package com.ishland.c2me.rewrites.chunkio;

import com.ishland.c2me.base.common.config.ConfigSystem;

public class ModuleEntryPoint {

    private static final boolean enabled = new ConfigSystem.ConfigAccessor()
            .key("ioSystem.replaceImpl")
            .comment("Whether to use the optimized implementation of IO system")
            .getBoolean(true, false);

    public static final String backend = new ConfigSystem.ConfigAccessor()
            .key("ioSystem.backend")
            .comment("The IO system backend to use, available: thread, vthread")
            .getString("thread", "thread");
}
