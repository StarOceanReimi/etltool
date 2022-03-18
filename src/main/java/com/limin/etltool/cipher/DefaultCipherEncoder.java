package com.limin.etltool.cipher;

import com.limin.etltool.util.Exceptions;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.SM4Engine;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.Random;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.ArrayUtils.swap;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2022/3/17
 */
public class DefaultCipherEncoder implements CipherEncoder {

    static final Pattern ENC_PATTERN = Pattern.compile("^ENC\\{(?<algo>[0-9]{2}):(?<secret>[A-Za-z0-9_-]+)}$");

    private final byte[] key;

    public DefaultCipherEncoder(String key) {
        this.key = Base64.getDecoder().decode(key);
    }

    public DefaultCipherEncoder(byte[] key) {
        this.key = key;
    }

    private static byte[] addSalt(byte[] src) {
        byte[] salt = new byte[src.length];
        new Random().nextBytes(salt);
        final byte[] result = new byte[src.length << 1];
        byte[] copy = Arrays.copyOf(src, src.length);
        for (int i = 0, len = salt.length; i < len; i++) {
            int j = len - i;
            int n = salt[i] & 0xff;
            int idx = i + (n % j);
            swap(copy, i, idx);
            result[i] = (byte) (salt[i] ^ copy[i]);
        }
        System.arraycopy(salt, 0, result, src.length, salt.length);
        return result;
    }

    @Override
    public String encodeString(String text) {
        if (Objects.isNull(text)) return null;
        if (isEncoded(text)) return text;
        return new LMGMEncoder(key, text).encode();
    }

    class LMGMEncoder {
        final byte[] key;
        String text;

        LMGMEncoder(byte[] key, String text) {
            this.key = key;
            this.text = text;
        }

        String encode() {
            try {
                byte[] result = doGMEncrypt(key, text);
                return formatSecret(LMGM, result);
            } catch (InvalidCipherTextException e) {
                throw Exceptions.propagate(e);
            }
        }
    }

    private String formatSecret(String algo, byte[] result) throws InvalidCipherTextException {
        return String.format("ENC{%s:%s}", algo,
                Base64.getUrlEncoder().withoutPadding().encodeToString(result));
    }

    private byte[] doGMEncrypt(byte[] key, String text) {
        SM4Engine sm4Engine = new SM4Engine();
        KeyParameter keyParameter = new KeyParameter(key);
        PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(sm4Engine);
        cipher.init(true, keyParameter);
        ByteArrayInputStream input = new ByteArrayInputStream(text.getBytes());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            CipherUtils.defaultBlockCipherFlow(cipher, input, output);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return output.toByteArray();
    }

    private boolean isEncoded(String text) {
        return ENC_PATTERN.matcher(text).matches();
    }

}
