package com.ishland.c2me.rewrites.chunkio.common;

import com.ibm.asyncutil.util.Either;
import com.ishland.c2me.rewrites.chunkio.ModuleEntryPoint;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import net.minecraft.nbt.NbtCompound;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class StoragePool {
    private static final ExecutorService STORAGE_POOL;
    private static final Scheduler VIRTUAL_SCHEDULER = Schedulers.from(Thread::startVirtualThread);

    static {
        final var selected = switch (ModuleEntryPoint.backend) {
            case "thread" -> Thread.ofPlatform().daemon();
            case "vthread" -> Thread.ofVirtual(); // Virtual threads can only be daemon threads
            default -> throw new IllegalStateException("Unexpected value: " + ModuleEntryPoint.backend);
        };
        STORAGE_POOL = Executors.newThreadPerTaskExecutor(
                selected.name("C2ME Storage #", 1).factory());
    }

    public static void runStorage(C2MEStorageHandle handle) {
        STORAGE_POOL.execute(new StorageWorker(handle));
    }

    public static Maybe<Either<NbtCompound, byte[]>> awaitVirtually(Supplier<NbtCompound> provider) {
        return Maybe.fromSupplier(() -> Either.<NbtCompound, byte[]>left(provider.get())).subscribeOn(VIRTUAL_SCHEDULER);
    }

    record StorageWorker(C2MEStorageHandle handle) implements Runnable {
        @Override
        public void run() {
            handle.initCarrier();
            handle.run();
        }
    }
}
