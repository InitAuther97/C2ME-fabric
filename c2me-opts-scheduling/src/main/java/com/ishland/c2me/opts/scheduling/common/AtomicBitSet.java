package com.ishland.c2me.opts.scheduling.common;

import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.BitSet;

public interface AtomicBitSet {
    static AtomicBitSet create(int length) {
        if (length <= 0) throw new IllegalArgumentException("length <= 0");
        if (length <= 32) return new AtomicBitSetInt((byte) length);
        if (length <= 64) return new AtomicBitSetLong((byte) length);
        return new AtomicBitSetLongMulti(length);
    }

    int length();
    void set(int index);
    boolean getAndSet(int index);
    boolean getAndClear(int index);
    IntIterator clearAndIterate();
    BitSet getAllAndClear();
}
