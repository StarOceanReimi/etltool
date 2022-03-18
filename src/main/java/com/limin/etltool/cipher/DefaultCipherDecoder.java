package com.limin.etltool.cipher;

import com.limin.etltool.util.Exceptions;
import org.bouncycastle.crypto.engines.SM4Engine;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.regex.Matcher;

import static com.limin.etltool.cipher.CipherEncoder.LMGM;
import static org.apache.commons.lang3.ArrayUtils.swap;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2022/3/17
 */
public class DefaultCipherDecoder implements CipherDecoder {

    private final byte[] key;

    public DefaultCipherDecoder(String key) {
        this.key = Base64.getDecoder().decode(key);
    }

    public DefaultCipherDecoder(byte[] key) {
        this.key = key;
    }

    private static byte[] removeSalt(byte[] salted) {
        int len = salted.length >> 1;
        byte[] result = Arrays.copyOf(salted, len);
        for (int i = 0; i < len; i++) {
            int j = len - 1 - i;
            result[j] = (byte) (salted[j] ^ salted[j + len]);
            int n = salted[j + len] & 0xff;
            int idx = j + n % (i + 1);
            swap(result, j, idx);
        }
        return result;
    }

    @Override
    public String decodeString(String text) {
        if (Objects.isNull(text)) return null;
        Encoded encoded = parseText(text);
        if (encoded == null)
            throw Exceptions.inform("invalid cipher text: {}", text);
        return doDecode(encoded);
    }

    private String doDecode(Encoded encoded) {
        if (LMGM.equals(encoded.algo)) {
            return new LMGMDecoder(encoded.secret).decode();
        }
        throw Exceptions.inform("non supported algo: {}", encoded.algo);
    }

    private Encoded parseText(String text) {
        Matcher m = DefaultCipherEncoder.ENC_PATTERN.matcher(text);
        if (!m.find()) return null;
        return new Encoded(m.group("algo"), m.group("secret"));
    }

    class LMGMDecoder {
        final byte[] secret;

        public LMGMDecoder(byte[] secret) {
            this.secret = secret;
        }

        public String decode() {
            return doDecrypt(key, secret);
        }

        private String doDecrypt(byte[] key, byte[] encoded) {
            SM4Engine sm4Engine = new SM4Engine();
            KeyParameter keyParameter = new KeyParameter(key);
            PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(sm4Engine);
            cipher.init(false, keyParameter);
            ByteArrayInputStream input = new ByteArrayInputStream(encoded);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            try {
                CipherUtils.defaultBlockCipherFlow(cipher, input, output);
                return new String(output.toByteArray());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    static class Encoded {
        byte[] secret;
        String algo;

        public Encoded(String algo, String secret) {
            this.algo = algo;
            this.secret = Base64.getUrlDecoder().decode(secret);
        }
    }
}
