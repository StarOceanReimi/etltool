package com.limin.etltool.excel.util;

import com.limin.etltool.util.Exceptions;

import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static java.util.Arrays.binarySearch;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/1/20
 */
public abstract class IndexUtils {

    private static BiConsumer<char[], char[]> operator = (c1, c2) -> {
        for (int i=0; i<c1.length; i++) {
            if(c2[i] >= 0x41 && c2[i] <= 0x5B)
                c1[i] = c2[i];
        }
    };

    private static final char[] LETTERS = IntStream.range(0, 26)
            .collect(() -> new char[26], (cs, i) -> cs[i] = (char) (i + 0x41), operator);

    private static BiConsumer<int[], int[]> combine = (c1, c2) -> {
    };


    public static String toIndexString(int num) {
        if (num < 1) throw Exceptions.unsupported("不支持数字小于1的序号");
        if (num < 26) return String.valueOf(LETTERS[num - 1]);
        int p = num / 26;
        return num % 26 == 0
                ? p == 1
                ? String.valueOf(LETTERS[(num - 1) % 26])
                : toIndexString(p - 1) + LETTERS[(num - 1) % 26]
                : toIndexString(p)     + LETTERS[(num - 1) % 26];
    }

    public static int indexStringValue(String value) {
        return value.chars()
                .map(i -> Character.isUpperCase((char) i) ? i : i - 32)
                .collect(StringBuilder::new, (s, i) -> s.append((char) i), StringBuilder::append)
                .reverse()
                .chars().collect(() -> new int[]{0, 0}, (a, c) -> {
                    a[1] += (1 + binarySearch(LETTERS, (char) c)) * Math.pow(26, a[0]);
                    a[0] += 1;
                }, combine)[1];
    }

}

