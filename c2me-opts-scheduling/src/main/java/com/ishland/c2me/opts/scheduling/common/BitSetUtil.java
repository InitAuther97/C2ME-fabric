package com.ishland.c2me.opts.scheduling.common;

import java.util.BitSet;

public class BitSetUtil {

    /**
     * Perform a logical or and return if anything is changed.
     * @param origin The BitSet to set all bits to
     * @param from The BitSet to set all bits from
     * @return if any fresh bits are set in origin
     */
    public static boolean setAll(BitSet origin, BitSet from) {
        BitSet check = (BitSet) from.clone();
        check.andNot(origin);
        if (check.isEmpty()) {
            return false;
        }
        origin.or(from);
        return true;
    }
}
