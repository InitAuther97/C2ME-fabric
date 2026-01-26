package com.ishland.c2me.rewrites.chunkio.common.vio;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VIOPool {
    public static final ExecutorService VIRTUAL_POOL = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("C2ME Storage #", 1).factory()
    );
}
