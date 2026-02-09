package com.ishland.c2me.opts.scheduling.common;

import it.unimi.dsi.fastutil.ints.IntIterator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.BitSet;

public class AtomicBitSetLongMulti implements AtomicBitSet {
    private final int length;
    private final long[] bits;

    public AtomicBitSetLongMulti(int length) {
        if (length <= 64) throw new IllegalArgumentException("Length is less than 64");
        this.length = length;
        this.bits = new long[1 + length >>> 6];
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public void set(int index) {
        if (index < 0 || index > length) throw new ArrayIndexOutOfBoundsException(index);
        final int idx = index >>> 6;
        final int shifted = 1 << (index & 63);
        VH_BITS.getAndBitwiseOr(this.bits, idx, shifted);
    }

    public boolean getAndSet(int index) {
        if (index < 0 || index > length) throw new ArrayIndexOutOfBoundsException(index);
        final int idx = index >>> 6;
        final long shifted = 1L << (index & 63);
        return 0 != (shifted & (long) VH_BITS.getAndBitwiseOr(this.bits, idx, shifted));
    }

    public boolean getAndClear(int index) {
        if (index < 0 || index > length) throw new ArrayIndexOutOfBoundsException(index);
        final int idx = index >>> 6;
        final long shifted = 1L << (index & 63);
        return 0 != (shifted & (long) VH_BITS.getAndBitwiseAnd(this.bits, idx, ~shifted));
    }

    @Override
    public IntIterator clearAndIterate() {
        final int size = 1 + length >>> 6;
        long[] copy = new long[size];
        int popCount = 0;
        for (int i = 0; i < size; i++) {
            final long read = (long) VH_BITS.getAndSet(this.bits, i, 0L);
            copy[i] = read;
            popCount += Long.bitCount(read);
        }
        return new Iterator(copy, popCount);
    }

    @Override
    public BitSet getAllAndClear() {
        final int size = 1 + length >>> 6;
        final long[] copy = new long[size];
        for (int i = 0; i < size; i++) {
            final long read = (long) VH_BITS.getAndSet(this.bits, i, 0L);
            copy[i] = read;
        }
        return BitSet.valueOf(copy);
    }

    static class Iterator implements IntIterator {

        private final int length;
        private final long[] bits;
        private int ptr;
        private int index = 0;

        Iterator(long[] bits, int popCount) {
            this.bits = bits;
            this.length = popCount;
        }

        @Override
        public int nextInt() {
            final int ptr = this.ptr;
            final long read = bits[ptr];
            final int result = Long.numberOfTrailingZeros(read);
            if (result == 64) {
                this.ptr = ptr + 1;
                return nextInt();
            } else {
                bits[ptr] = read & read - 1;
                this.index++;
                return result + ptr << 6;
            }
        }

        @Override
        public boolean hasNext() {
            return index < length;
        }
    }

    private static final VarHandle VH_BITS;
    static {
        VH_BITS = MethodHandles.arrayElementVarHandle(long[].class);
    }
}
