package com.limin.etltool.step;

import com.limin.etltool.cipher.DefaultCipherEncoder;

import java.util.Arrays;

import static java.util.Optional.ofNullable;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2022/3/18
 */
public abstract class EncryptEditors {

    public static <T> ColumnEditing<T> makeEditor(String[] columnNames,
                                                  String encryptKey) {
        ColumnEditing<T> editing = new ColumnEditing<>();
        final DefaultCipherEncoder encoder = new DefaultCipherEncoder(encryptKey);
        Arrays.stream(columnNames).forEach(name -> editing
                .registerEditor(name, v -> ofNullable(v)
                        .filter(x -> x instanceof String)
                        .map(String.class::cast)
                        .map(encoder::encodeString).orElse(null)));
        return editing;
    }

}
