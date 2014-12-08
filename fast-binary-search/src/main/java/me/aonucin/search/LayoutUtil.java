package me.aonucin.search;

import java.util.Arrays;

public class LayoutUtil {

    public static int log2(int integer) {
        if(integer == 0) {
            return 0;
        }
        return 31 - Integer.numberOfLeadingZeros(integer);
    }

    public static int powerOfTwo(int i) {
        return  1 << i;
    };

}
