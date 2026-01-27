package com.ishland.c2me.rewrites.chunkio.common;

import com.ibm.asyncutil.util.Either;
import io.reactivex.rxjava3.core.Maybe;
import net.minecraft.nbt.NbtCompound;
import net.minecraft.util.math.ChunkPos;
import org.jetbrains.annotations.NotNull;

public class WriteCache {

    static final Exception OUTDATED = new Exception("Future is outdated");

    final ChunkPos pos;
    @NotNull
    private final Maybe<Either<NbtCompound, byte[]>> dataFuture;
    State state = State.CANCELLED;

    public WriteCache(long pos, Maybe<Either<NbtCompound, byte[]>> dataFuture) {
        this.pos = new ChunkPos(pos);
        this.dataFuture = dataFuture;
    }

    public void cancel() {
        if (this.state == State.WRITING) {
            return;
        }
        this.state = State.CANCELLED; // For cache reading
    }

    public boolean isCancelled() {
        return state == State.CANCELLED;
    }

    public Maybe<Either<NbtCompound, byte[]>> get() {
        return dataFuture;
    }

    enum State {
        DEAD,
        CANCELLED,
        AWAIT_DATA,
        AWAIT_WRITE,
        WRITING
    }
}
