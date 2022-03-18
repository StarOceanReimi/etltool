package com.limin.etltool.cipher;

import com.google.common.base.Strings;
import com.limin.etltool.util.Exceptions;
import org.apache.commons.lang3.ArrayUtils;
import org.bouncycastle.asn1.ASN1Encoding;
import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.pkcs.*;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.asn1.x9.ECNamedCurveTable;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.*;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.encodings.OAEPEncoding;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.engines.RSAEngine;
import org.bouncycastle.crypto.engines.SM2Engine;
import org.bouncycastle.crypto.generators.DHKeyPairGenerator;
import org.bouncycastle.crypto.generators.DHParametersGenerator;
import org.bouncycastle.crypto.generators.ECKeyPairGenerator;
import org.bouncycastle.crypto.generators.RSAKeyPairGenerator;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.params.*;
import org.bouncycastle.crypto.signers.RSADigestSigner;
import org.bouncycastle.crypto.signers.SM2Signer;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.crypto.util.PublicKeyFactory;
import org.bouncycastle.jcajce.provider.asymmetric.dh.BCDHPrivateKey;
import org.bouncycastle.jcajce.provider.asymmetric.dh.BCDHPublicKey;
import org.bouncycastle.jcajce.provider.asymmetric.util.KeyUtil;
import org.bouncycastle.math.ec.ECPoint;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author 邱理
 * @since 2019/6/13
 */
public abstract class CipherUtils {

    private static final String HmacSHA265 = "HmacSHA256";
    private static final String LETTERS = "0123456789abcdef";

    private static final X9ECParameters x9Param = ECNamedCurveTable.getByName("sm2p256v1");
    private static final ECDomainParameters ecDomainParameters = new ECDomainParameters(
            x9Param.getCurve(), x9Param.getG(), x9Param.getN(),
            x9Param.getH(), x9Param.getSeed());


    public static AsymmetricCipherKeyPair generateKeyPair(SecureRandom random) {
        ECKeyPairGenerator gen = new ECKeyPairGenerator();
        gen.init(new ECKeyGenerationParameters(ecDomainParameters, random));
        return gen.generateKeyPair();
    }


    public static ECPublicKeyParameters readECPublicPemKey(String publicKey) throws IOException {
        try (PEMParser pemParser = new PEMParser(new StringReader(publicKey))) {
            SubjectPublicKeyInfo publicKeyInfo = (SubjectPublicKeyInfo) pemParser.readObject();
            return (ECPublicKeyParameters) PublicKeyFactory.createKey(publicKeyInfo);
        }
    }

    public static ECPrivateKeyParameters readECPrivatePemKey(String privateKey) throws IOException {
        try (PEMParser pemParser = new PEMParser(new StringReader(privateKey))) {
            PrivateKeyInfo privateKeyInfo = (PrivateKeyInfo) pemParser.readObject();
            return (ECPrivateKeyParameters) PrivateKeyFactory.createKey(privateKeyInfo);
        }
    }

    public static OutputStream wrapBuffered(OutputStream os) {
        if (os instanceof BufferedOutputStream ||
                os instanceof ByteArrayOutputStream) return os;
        return new BufferedOutputStream(os);
    }

    public static void defaultBlockCipherFlow(
            BufferedBlockCipher cipher,
            InputStream inputStream,
            OutputStream outputStream) throws IOException {
        try (OutputStream os = wrapBuffered(outputStream)) {
            int read, len;
            int size = cipher.getBlockSize();
            byte[] buffer = new byte[size];
            //size * 2 is for padded buffer block cipher
            byte[] out = new byte[size * 2];
            while ((read = inputStream.read(buffer, 0, buffer.length)) != -1) {
                len = cipher.getUpdateOutputSize(read);
                cipher.processBytes(buffer, 0, read, out, 0);
                if (len > 0) os.write(out, 0, len);
            }
            len = cipher.doFinal(out, 0);
            os.write(out, 0, len);
        } catch (InvalidCipherTextException e) {
            throw new RuntimeException(e);
        }
    }


    public static byte[] getECPubKeyEncoded(AsymmetricCipherKeyPair keyPair) {
        ECPublicKeyParameters pub = (ECPublicKeyParameters) keyPair.getPublic();
        BigInteger gx = pub.getQ().getAffineXCoord().toBigInteger();
        BigInteger gy = pub.getQ().getAffineYCoord().toBigInteger();
        byte[] gxB = gx.toByteArray();
        byte[] gyB = gy.toByteArray();
        ByteBuffer pubKeyBuf = ByteBuffer.allocate(2 + gxB.length + gyB.length);
        pubKeyBuf.put((byte) gxB.length);
        pubKeyBuf.put((byte) gyB.length);
        pubKeyBuf.put(gxB);
        pubKeyBuf.put(gyB);
        return pubKeyBuf.array();
    }

    public static byte[] getECPrivateKeyEncoded(AsymmetricCipherKeyPair keyPair) {
        ECPrivateKeyParameters prv = (ECPrivateKeyParameters) keyPair.getPrivate();
        return prv.getD().toByteArray();
    }

    public static byte[] sm2Sign(byte[] privateKey, byte[] content) {
        SM2Signer sm2Signer = new SM2Signer();
        sm2Signer.init(true, new ECPrivateKeyParameters(new BigInteger(privateKey), ecDomainParameters));
        sm2Signer.update(content, 0, content.length);
        try {
            return sm2Signer.generateSignature();
        } catch (CryptoException e) {
            Exceptions.rethrow(e);
            return null;
        }
    }

    /**
     * sm2签名验证方法
     *
     * @param publicKey 公钥
     * @param sig       签名
     * @param content   签名内容
     * @return 验签成功
     */
    public static boolean sm2Verify(byte[] publicKey, byte[] sig, byte[] content) {
        SM2Signer sm2Signer = new SM2Signer();
        ECPoint pubPoint = getEcPubPointDecoded(publicKey);
        sm2Signer.init(false, new ECPublicKeyParameters(pubPoint, ecDomainParameters));
        sm2Signer.update(content, 0, content.length);
        return sm2Signer.verifySignature(sig);
    }

    /**
     * sm2公钥加密
     *
     * @param publicKey
     * @param content
     * @return
     */
    public static String sm2Encrypt(String publicKey, byte[] content) {
        SM2Engine sm2Engine = new SM2Engine();
        ECPoint pubPoint = getEcPubPointDecoded(Base64.getDecoder().decode(publicKey));
        sm2Engine.init(true, new ParametersWithRandom(new ECPublicKeyParameters(pubPoint, ecDomainParameters)));
        String ciphertext = "";
        try {
            byte[] ciphertextByte = sm2Engine.processBlock(content, 0, content.length);
            ciphertext = Base64.getEncoder().encodeToString(ciphertextByte);
        } catch (InvalidCipherTextException e) {
            Exceptions.rethrow(e);
        }
        return ciphertext;
    }

    /**
     * sm2私钥解密
     *
     * @param privateKey
     * @param ciphertext
     * @return
     */
    public static String sm2Decrypt(String privateKey, String ciphertext) {
        SM2Engine sm2Engine = new SM2Engine();
        sm2Engine.init(false, new ECPrivateKeyParameters(new BigInteger(Base64.getDecoder().decode(privateKey)), ecDomainParameters));
        String realContent = "";
        try {
            byte[] c = Base64.getDecoder().decode(ciphertext);
            realContent = new String(sm2Engine.processBlock(c, 0, c.length));
        } catch (InvalidCipherTextException e) {
            Exceptions.rethrow(e);
        }
        return realContent;
    }

    private static ECPoint getEcPubPointDecoded(byte[] publicKey) {
        ByteBuffer pubBuf = ByteBuffer.wrap(publicKey);
        int gxL = pubBuf.get();
        int gyL = pubBuf.get();
        byte[] gxB = new byte[gxL];
        byte[] gyB = new byte[gyL];
        pubBuf.get(gxB, 0, gxL);
        pubBuf.get(gyB, 0, gyL);
        BigInteger gx = new BigInteger(gxB);
        BigInteger gy = new BigInteger(gyB);
        return x9Param.getCurve().createPoint(gx, gy);
    }

    public static String toHexString(byte[] data) {
        BiConsumer<StringBuilder, Byte> collector = (acc, b) -> {
            int hi = (b & 0xf0) >> 4;
            int lo = (b & 0x0f);
            acc.append(LETTERS.charAt(hi)).append(LETTERS.charAt(lo));
        };
        return IntStream.range(0, data.length)
                .mapToObj(i -> data[i])
                .collect(StringBuilder::new, collector, StringBuilder::append)
                .toString();
    }

    public static byte[] messageDigest(String algorithm, byte[] source) {
        try {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            return digest.digest(source);
        } catch (NoSuchAlgorithmException e) {
            throw Exceptions.inform("no such algorithm: %s", algorithm);
        }
    }

    public static byte[] sha1(byte[] source) {
        return messageDigest("sha1", source);
    }

    public static byte[] hmacSHA256(byte[] sk, byte[] message) {
        try {
            Mac mac = Mac.getInstance(HmacSHA265);
            SecretKeySpec spec = new SecretKeySpec(sk, HmacSHA265);
            mac.init(spec);
            return mac.doFinal(message);
        } catch (InvalidKeyException | NoSuchAlgorithmException e) {
            Exceptions.rethrow(e);
            return null;
        }
    }

    private static final byte[] EMPTY_BYTES = new byte[0];

    public static byte[] pkcs7pad(byte[] src, int blockSize) {
        if (src == null) return EMPTY_BYTES;
        if (src.length == 0) return src;
        int len = src.length;
        int pad = blockSize - len % blockSize;
        ByteBuffer buf = ByteBuffer.allocate(len + pad);
        buf.put(src);
        int count = 0;
        while (++count <= pad) buf.put((byte) pad);
        return buf.array();
    }

    public static byte[] removePkcs7Pad(byte[] src) {
        if (src == null) return EMPTY_BYTES;
        if (src.length == 0) return src;
        int padSize = src[src.length - 1];
        if (src.length <= padSize) return src;
        byte[] copy = new byte[src.length - padSize];
        System.arraycopy(src, 0, copy, 0, copy.length);
        return copy;
    }

    private static void blockCipherFlow(BlockCipher cipher, InputStream inputStream, OutputStream outputStream) throws IOException {
        int len;
        byte[] buf = new byte[cipher.getBlockSize()];
        while ((len = inputStream.read(buf, 0, buf.length)) != -1) {
            cipher.processBlock(buf, 0, buf, 0);
            outputStream.write(buf, 0, len);
        }
    }

    private static void blockCipherFlow(
            BufferedBlockCipher cipher,
            InputStream inputStream,
            OutputStream outputStream) throws IOException {
        int read, len;
        byte[] buffer = new byte[cipher.getBlockSize()];
        while ((read = inputStream.read(buffer, 0, buffer.length)) != -1) {
            len = cipher.getUpdateOutputSize(read);
            if (len != 0) {
                byte[] out = new byte[len];
                cipher.processBytes(buffer, 0, read, out, 0);
                outputStream.write(out);
            } else {
                cipher.processBytes(buffer, 0, read, null, 0);
            }
        }
        try {
            len = cipher.doFinal(buffer, 0);
            outputStream.write(buffer, 0, len);
        } catch (InvalidCipherTextException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] aesEncrypt(byte[] key, byte[] plain) {
        AESEngine engine = new AESEngine();
        engine.init(true, new KeyParameter(key));
        byte[] padded = pkcs7pad(plain, engine.getBlockSize());
        ByteArrayInputStream input = new ByteArrayInputStream(padded);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            blockCipherFlow(engine, input, output);
        } catch (IOException e) {
            Exceptions.rethrow(e);
        }
        return output.toByteArray();
    }

    public static byte[] aesDecrypt(byte[] key, byte[] encrypted) {
        AESEngine engine = new AESEngine();
        engine.init(false, new KeyParameter(key));
        ByteArrayInputStream input = new ByteArrayInputStream(encrypted);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            blockCipherFlow(engine, input, output);
        } catch (IOException e) {
            Exceptions.rethrow(e);
        }
        return removePkcs7Pad(output.toByteArray());
    }

    public static byte[] aesEncryptCBC(byte[] key, byte[] iv, byte[] plain) {
        AESEngine engine = new AESEngine();
        byte[] padded = pkcs7pad(plain, engine.getBlockSize());
        ByteArrayInputStream input = new ByteArrayInputStream(padded);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        CBCBlockCipher cbc = new CBCBlockCipher(engine);
        ParametersWithIV parametersWithIV = new ParametersWithIV(new KeyParameter(key), iv);
        cbc.init(true, parametersWithIV);
        try {
            blockCipherFlow(cbc, input, output);
        } catch (IOException e) {
            Exceptions.rethrow(e);
        }
        return output.toByteArray();
    }

    public static byte[] aesDecryptCBC(byte[] key, byte[] iv, byte[] encrypted) {
        AESEngine engine = new AESEngine();
        ByteArrayInputStream input = new ByteArrayInputStream(encrypted);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        CBCBlockCipher cbc = new CBCBlockCipher(engine);
        ParametersWithIV parametersWithIV = new ParametersWithIV(new KeyParameter(key), iv);
        cbc.init(false, parametersWithIV);
        try {
            blockCipherFlow(cbc, input, output);
        } catch (IOException e) {
            Exceptions.rethrow(e);
        }
        return removePkcs7Pad(output.toByteArray());
    }

    public static String hex(byte[] bytes) {
        return IntStream.range(0, bytes.length).map(i -> bytes[i] & 0xFF)
                .mapToObj(i -> i > 15 ? Integer.toHexString(i) : "0" + Integer.toHexString(i))
                .collect(Collectors.joining(""));
    }

    public static byte[] hex(String hex) {
        if ((hex.length() & 1) == 1)
            throw new IllegalArgumentException("hex length can not be odd.");
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < hex.length(); i += 2) {
            byte hi = (byte) ((Character.digit(hex.charAt(i), 16) << 4) & 0xF0);
            byte low = (byte) (Character.digit(hex.charAt(i + 1), 16) & 0xF);
            bytes[i / 2] = (byte) (hi | low);
        }
        return bytes;
    }

    //FROM bouncy castle package
    private static byte[] rfc2631Padding(int primeBits, BigInteger r) {
        //
        // RFC 2631 (2.1.2) specifies that the secret should be padded with leading zeros if necessary
        // must be the same length as p
        //
        int expectedLength = (primeBits + 7) / 8;

        byte[] tmp = r.toByteArray();

        if (tmp.length == expectedLength) {
            return tmp;
        }

        if (tmp[0] == 0 && tmp.length == expectedLength + 1) {
            byte[] rv = new byte[tmp.length - 1];

            System.arraycopy(tmp, 1, rv, 0, rv.length);
            return rv;
        }

        // tmp must be shorter than expectedLength
        // pad to the left with zeros.
        byte[] rv = new byte[expectedLength];

        System.arraycopy(tmp, 0, rv, rv.length - tmp.length, tmp.length);

        return rv;
    }

    private static byte[] extractKey(String s) {
        return Arrays.copyOf(Base64.getDecoder().decode(s), 32);
    }

    public static AsymmetricCipherKeyPair readRsaAsymmetricKeyPairInPemFormat(String privateKey) throws IOException {
        PEMParser pemParser = new PEMParser(new StringReader(privateKey));
        PEMKeyPair keyPair = (PEMKeyPair) pemParser.readObject();
        RSAPrivateKey rsaPrivateKey = RSAPrivateKey.getInstance(keyPair.getPrivateKeyInfo().parsePrivateKey());
        RSAPrivateCrtKeyParameters privateCrtKeyParameters = new RSAPrivateCrtKeyParameters(
                rsaPrivateKey.getModulus(),
                rsaPrivateKey.getPublicExponent(),
                rsaPrivateKey.getPrivateExponent(),
                rsaPrivateKey.getPrime1(),
                rsaPrivateKey.getPrime2(),
                rsaPrivateKey.getExponent1(),
                rsaPrivateKey.getExponent2(),
                rsaPrivateKey.getCoefficient());
        RSAKeyParameters publicKeyParameters = new RSAKeyParameters(false,
                rsaPrivateKey.getModulus(), rsaPrivateKey.getPublicExponent());
        return new AsymmetricCipherKeyPair(publicKeyParameters, privateCrtKeyParameters);
    }

    public static String rsaTrustClientSign(RSAKeyParameters privateKey) throws CryptoException {
        checkNotNull(privateKey);
        RSADigestSigner rsaSigner = new RSADigestSigner(new SHA256Digest());
        byte[] content = new byte[16];
        secureRandom.nextBytes(content);
        rsaSigner.init(true, privateKey);
        rsaSigner.update(content, 0, content.length);
        byte[] sig = rsaSigner.generateSignature();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(ArrayUtils.addAll(content, sig));
    }

    public static boolean rsaVerifyClientSign(String payload, RSAKeyParameters publicKey) {
        checkNotNull(publicKey);
        if (Strings.isNullOrEmpty(payload) || payload.length() < 2) return false;
        RSADigestSigner rsaSigner = new RSADigestSigner(new SHA256Digest());
        try {
            byte[] contentWithSig = Base64.getUrlDecoder().decode(payload);
            if (contentWithSig.length < 17) return false;
            byte[] content = Arrays.copyOfRange(contentWithSig, 0, 16);
            byte[] sig = Arrays.copyOfRange(contentWithSig, 16, contentWithSig.length);
            rsaSigner.init(false, publicKey);
            rsaSigner.update(content, 0, content.length);
            return rsaSigner.verifySignature(sig);
        } catch (Exception ex) {
            return false;
        }
    }

    public static RSAKeyParameters readRSAPublicKeyInPemFormat(String publicKey) throws IOException {
        PEMParser pemParser = new PEMParser(new StringReader(publicKey));
        SubjectPublicKeyInfo publicKeyInfo = (SubjectPublicKeyInfo) pemParser.readObject();
        RSAPublicKey rsaPublicKey = RSAPublicKey.getInstance(publicKeyInfo.parsePublicKey());
        return new RSAKeyParameters(false, rsaPublicKey.getModulus(), rsaPublicKey.getPublicExponent());
    }

    public static Optional<String> generateRSAPublicKeyFromPrivateKeyInPemFormat(String privateKey) {
        PEMParser pemParser = new PEMParser(new StringReader(privateKey));
        try {
            PEMKeyPair pemKeyPair = (PEMKeyPair) pemParser.readObject();
            StringWriter stringWriter = new StringWriter();
            PemWriter writer = new PemWriter(stringWriter);
            writer.writeObject(new PemObject("PUBLIC KEY", pemKeyPair.getPublicKeyInfo().getEncoded()));
            writer.close();
            return Optional.of(stringWriter.toString());
        } catch (IOException e) {
            //ignore
        }
        return Optional.empty();
    }

    private static final BigInteger E = BigInteger.valueOf(65537);
    private static final BigInteger G = BigInteger.valueOf(2);
    private static final SecureRandom secureRandom = new SecureRandom();

    public static Optional<String> obtainDHSharedSecretFromPublic(String dhPublicKey, String dhPrivateKey) {
        PEMParser publicParser = new PEMParser(new StringReader(dhPublicKey));
        PEMParser privateParser = new PEMParser(new StringReader(dhPrivateKey));
        try {
            SubjectPublicKeyInfo publicKeyInfo = (SubjectPublicKeyInfo) publicParser.readObject();
            BCDHPublicKey publicKey = new BCDHPublicKey(publicKeyInfo);
            PrivateKeyInfo privateKeyInfo = (PrivateKeyInfo) privateParser.readObject();
            BCDHPrivateKey privateKey = new BCDHPrivateKey(privateKeyInfo);
            BigInteger p = publicKey.getParams().getP();
            BigInteger r = publicKey.getY().modPow(privateKey.getX(), publicKey.getParams().getP());
            return Optional.of(Base64.getEncoder().encodeToString(rfc2631Padding(p.bitLength(), r)));
        } catch (Exception ex) {
            //ignore
        }
        return Optional.empty();
    }

    public static String generateDHPrivateKeyFromPublicKeyInPemFormat(String publicKey) throws IOException {

        PEMParser pemParser = new PEMParser(new StringReader(publicKey));
        SubjectPublicKeyInfo subjectPublicKeyInfo = (SubjectPublicKeyInfo) pemParser.readObject();
        BCDHPublicKey bcdhPublicKey = new BCDHPublicKey(subjectPublicKeyInfo);
        BigInteger p = bcdhPublicKey.getParams().getP();
        BigInteger g = bcdhPublicKey.getParams().getG();
        DHKeyPairGenerator dhKeyPairGenerator = new DHKeyPairGenerator();
        DHParameters dhParameters = new DHParameters(p, g);
        DHKeyGenerationParameters generationParameters = new DHKeyGenerationParameters(secureRandom, dhParameters);
        dhKeyPairGenerator.init(generationParameters);
        DHPrivateKeyParameters privateParameters = (DHPrivateKeyParameters)
                dhKeyPairGenerator.generateKeyPair().getPrivate();
        StringWriter sw = new StringWriter();
        PemWriter writer = new PemWriter(sw);
        DHParameter dhParameter = new DHParameter(p, g, 0);
        PrivateKeyInfo privateKeyInfo = new PrivateKeyInfo(
                new AlgorithmIdentifier(PKCSObjectIdentifiers.dhKeyAgreement, dhParameter.toASN1Primitive()),
                new ASN1Integer(privateParameters.getX()));
        writer.writeObject(new PemObject("PRIVATE KEY", privateKeyInfo.getEncoded(ASN1Encoding.DER)));
        writer.flush();
        writer.close();
        return sw.toString();

    }

    public static Optional<String> generateDHPublicKeyFromPrivateKeyInPemFormat(String privateKey) {
        PEMParser pemParser = new PEMParser(new StringReader(privateKey));
        try {
            PrivateKeyInfo dhPrivate = (PrivateKeyInfo) pemParser.readObject();
            BCDHPrivateKey bcdhPrivateKey = new BCDHPrivateKey(dhPrivate);
            StringWriter stringWriter = new StringWriter();

            BigInteger p = bcdhPrivateKey.getParams().getP();
            BigInteger g = bcdhPrivateKey.getParams().getG();
            BigInteger y = g.modPow(bcdhPrivateKey.getX(), p);

            byte[] pub = KeyUtil.getEncodedSubjectPublicKeyInfo(
                    new AlgorithmIdentifier(PKCSObjectIdentifiers.dhKeyAgreement, new DHParameter(
                            p, g, bcdhPrivateKey.getParams().getL()).toASN1Primitive()),
                    new ASN1Integer(y));
            PemWriter writer = new PemWriter(stringWriter);
            writer.writeObject(new PemObject("PUBLIC KEY", pub));
            writer.close();
            return Optional.of(stringWriter.toString());
        } catch (IOException e) {
            //ignore
        }
        return Optional.empty();
    }

    public static String generateDHPrivateKeyInPemFormat(int keyStrength, int certainty) throws IOException {

        DHParametersGenerator dhParametersGenerator = new DHParametersGenerator();
        dhParametersGenerator.init(keyStrength, certainty, secureRandom);
        DHParameters dhParameters = dhParametersGenerator.generateParameters();
        DHKeyPairGenerator keyPairGenerator = new DHKeyPairGenerator();
        DHParameters dhParamForKeyExchange = new DHParameters(dhParameters.getP(), G);
        keyPairGenerator.init(new DHKeyGenerationParameters(secureRandom, dhParamForKeyExchange));
        AsymmetricCipherKeyPair keyPair = keyPairGenerator.generateKeyPair();
        DHPrivateKeyParameters dhPrivateKeyParameters = (DHPrivateKeyParameters) keyPair.getPrivate();

        PrivateKeyInfo privateKeyInfo = new PrivateKeyInfo(
                new AlgorithmIdentifier(PKCSObjectIdentifiers.dhKeyAgreement,
                        new DHParameter(dhParamForKeyExchange.getP(), dhParamForKeyExchange.getG(),
                                dhParamForKeyExchange.getL()).toASN1Primitive()),
                new ASN1Integer(dhPrivateKeyParameters.getX()));

        StringWriter sw = new StringWriter();
        PemWriter writer = new PemWriter(sw);
        writer.writeObject(new PemObject("PRIVATE KEY", privateKeyInfo.getEncoded(ASN1Encoding.DER)));
        writer.flush();
        writer.close();
        return sw.toString();
    }

    public static String generateRSAPrivateKeyInPemFormat(int keyStrength, int certainty) throws IOException {

        RSAKeyPairGenerator keyPairGenerator = new RSAKeyPairGenerator();
        RSAKeyGenerationParameters generationParameters =
                new RSAKeyGenerationParameters(E, secureRandom, keyStrength, certainty);
        keyPairGenerator.init(generationParameters);
        AsymmetricCipherKeyPair keyPair = keyPairGenerator.generateKeyPair();
        RSAPrivateCrtKeyParameters privateCrtKeyParameters = (RSAPrivateCrtKeyParameters) keyPair.getPrivate();

        RSAPrivateKey privateKey = new RSAPrivateKey(
                privateCrtKeyParameters.getModulus(),
                privateCrtKeyParameters.getPublicExponent(),
                privateCrtKeyParameters.getExponent(),
                privateCrtKeyParameters.getP(),
                privateCrtKeyParameters.getQ(),
                privateCrtKeyParameters.getDP(),
                privateCrtKeyParameters.getDQ(),
                privateCrtKeyParameters.getQInv());

        StringWriter sw = new StringWriter();
        PemWriter writer = new PemWriter(sw);
        writer.writeObject(new PemObject("RSA PRIVATE KEY", privateKey.getEncoded()));
        writer.close();

        return sw.toString();

    }

    public static String rsaOaepEncode(RSAKeyParameters key, String plain) throws InvalidCipherTextException, IOException {
        return rsaOaepEncode(key, plain, new SHA256Digest());
    }

    public static String rsaOaepEncode(RSAKeyParameters key, String plain, Digest digest) throws InvalidCipherTextException, IOException {
        RSAEngine engine = new RSAEngine();
        OAEPEncoding encoder = new OAEPEncoding(engine, digest);
        encoder.init(true, key);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(plain.getBytes());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buf = new byte[encoder.getInputBlockSize()];
        int len;
        while ((len = inputStream.read(buf, 0, buf.length)) != -1) {
            byte[] out = encoder.processBlock(buf, 0, len);
            outputStream.write(out);
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(outputStream.toByteArray());
    }

    public static String rsaOaepDecode(RSAKeyParameters key, String encrypted) throws IOException, InvalidCipherTextException {
        return rsaOaepDecode(key, encrypted, new SHA256Digest());
    }

    public static String rsaOaepDecode(RSAKeyParameters key, String encrypted, Digest digest) throws IOException, InvalidCipherTextException {
        RSAEngine engine = new RSAEngine();
        OAEPEncoding encoder = new OAEPEncoding(engine, digest);
        encoder.init(false, key);
        InputStream inputStream = new ByteArrayInputStream(Base64.getUrlDecoder().decode(encrypted));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int bs = encoder.getInputBlockSize();
        byte[] buf = new byte[bs];
        int len;
        while ((len = inputStream.read(buf, 0, buf.length)) != -1) {
            byte[] out = encoder.processBlock(buf, 0, len);
            outputStream.write(out);
        }
        return new String(outputStream.toByteArray());
    }

}
