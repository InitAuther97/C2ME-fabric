package com.ishland.c2me.opts.scheduling.common;

import it.unimi.dsi.fastutil.ints.IntIterator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.BitSet;

public class AtomicBitSetInt implements AtomicBitSet {
    private final byte length;
    private volatile int bits = 0;

    public AtomicBitSetInt(byte length) {
        this.length = length;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public void set(int index) {
        if (index < 0 || index > length) throw new ArrayIndexOutOfBoundsException(index);
        final int shifted = 1 << index;
        VH_BITS.getAndBitwiseOr(this, shifted);
    }

    public boolean getAndSet(int index) {
        if (index < 0 || index > length) throw new ArrayIndexOutOfBoundsException(index);
        final int shifted = 1 << index;
        return 0 != (shifted & (int) VH_BITS.getAndBitwiseOr(this, shifted));
    }

    public boolean getAndClear(int index) {
        if (index < 0 || index > length) throw new ArrayIndexOutOfBoundsException(index);
        final int shifted = 1 << index;
        return 0 != (shifted & (int) VH_BITS.getAndBitwiseAnd(this, ~shifted));
    }

    @Override
    public IntIterator clearAndIterate() {
        return new Iterator((int) VH_BITS.getAndSet(this, 0));
    }

    @Override
    public BitSet getAllAndClear() {
        final int value = (int) VH_BITS.getAndSet(this, 0);
        return BitSet.valueOf(new long[]{value});
    }

    static class Iterator implements IntIterator {

        private final int length;
        private int bits;
        private int index = 0;

        Iterator(int bits) {
            this.bits = bits;
            this.length = Integer.bitCount(bits);
        }

        @Override
        public int nextInt() {
            final int bits = this.bits;
            final int result = Integer.numberOfTrailingZeros(bits);
            this.bits &= bits - 1;
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
            VH_BITS = MethodHandles.lookup().findVarHandle(AtomicBitSetInt.class, "bits", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
