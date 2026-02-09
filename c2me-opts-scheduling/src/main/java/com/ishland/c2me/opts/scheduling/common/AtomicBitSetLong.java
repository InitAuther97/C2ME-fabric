package com.ishland.c2me.opts.scheduling.common;

import it.unimi.dsi.fastutil.ints.IntIterator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.BitSet;

public class AtomicBitSetLong implements AtomicBitSet {
    private final byte length;
    private volatile long bits = 0;

    public AtomicBitSetLong(byte length) {
        if (length < 0 || length > 63) throw new IllegalArgumentException("length must be between 0 and 63");
        this.length = length;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public void set(int index) {
        if (index < 0 || index > length) throw new ArrayIndexOutOfBoundsException(index);
        final long shifted = 1L << index;
        VH_BITS.getAndBitwiseOr(this, shifted);
    }

    public boolean getAndSet(int index) {
        if (index < 0 || index > length) throw new ArrayIndexOutOfBoundsException(index);
        final long shifted = 1L << index;
        return 0 != (shifted & (long) VH_BITS.getAndBitwiseOr(this, shifted));
    }

    public boolean getAndClear(int index) {
        if (index < 0 || index > length) throw new ArrayIndexOutOfBoundsException(index);
        final long shifted = 1L << index;
        return 0 != (shifted & (long) VH_BITS.getAndBitwiseAnd(this, ~shifted));
    }

    @Override
    public IntIterator clearAndIterate() {
        return new Iterator((long) VH_BITS.getAndSet(this, 0L));
    }

    @Override
    public BitSet getAllAndClear() {
        final long value = (long) VH_BITS.getAndSet(this, 0L);
        return BitSet.valueOf(new long[]{value});
    }

    static class Iterator implements IntIterator {

        private final int length;
        private long bits;
        private int index = 0;

        Iterator(long bits) {
            this.bits = bits;
            this.length = Long.bitCount(bits);
        }

        @Override
        public int nextInt() {
            final int result = Long.numberOfTrailingZeros(bits);
            bits &= bits - 1;
            index++;
            return result;
        }

        @Override
        public boolean hasNext() {
            return index < length;
        }
    }

    private static final VarHandle VH_BITS;
    static {
        try {
            VH_BITS = MethodHandles.lookup().findVarHandle(AtomicBitSetLong.class, "bits", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
