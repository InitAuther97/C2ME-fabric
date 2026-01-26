package com.ishland.c2me.rewrites.chunkio.common;

import com.ibm.asyncutil.util.Either;
import com.ishland.c2me.rewrites.chunkio.ModuleEntryPoint;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import net.minecraft.nbt.NbtCompound;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class StoragePool {
    private static final ExecutorService STORAGE_POOL;
    private static final Scheduler VIRTUAL_SCHEDULER = Schedulers.from(Thread::startVirtualThread);

    static {
        final var selected = switch (ModuleEntryPoint.backend) {
            case "thread" -> Thread.ofPlatform().daemon();
            case "vthread" -> Thread.ofVirtual();
            default -> throw new IllegalStateException("Unexpected value: " + ModuleEntryPoint.backend);
        };
        STORAGE_POOL = Executors.newThreadPerTaskExecutor(
                selected.name("C2ME Storage #", 1).factory());
    }

    public static CompletableFuture<?> runStorage(C2MEStorageHandle storage) {
        return CompletableFuture.runAsync(storage, STORAGE_POOL)
                .whenComplete((unused, th) -> {
                    if (th != null) {
                        C2MEStorageHandle.LOGGER.error("Storage {} crashed", storage, th);
                    } else {
                        C2MEStorageHandle.LOGGER.info("Storage {} finished execution", storage);
                    }
                });
    }

    public static Maybe<Either<NbtCompound, byte[]>> awaitVirtually(Supplier<NbtCompound> provider) {
        return Maybe.just(provider).observeOn(VIRTUAL_SCHEDULER).map(Supplier::get).map(Either::left);
    }
}
