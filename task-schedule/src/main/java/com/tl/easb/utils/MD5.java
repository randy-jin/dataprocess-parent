package com.tl.easb.utils;

/**
 * Created by huangchunhuai on 2021/1/8.
 */

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.security.SecureRandom;

public class MD5 {
    /**
     * 偏移变量，固定占8位字节
     */
    private final static String IV_PARAMETER = "12345678";
    /**
     * 密钥算法
     */
    private static final String ALGORITHM = "DES";
    /**
     * 默认编码
     */
    private static final String CHARSET = "utf-8";

    /**
     * DES加密字符串
     *
     * @param password 加密密码，长度不能够小于8位
     * @return 加密后内容
     */
    public static String encrypt(String password) {
        if (password== null || password.length() < 8) {
            throw new RuntimeException("加密失败，key不能小于8位");
        }

        try {
            SecureRandom random = new SecureRandom();
            DESKeySpec keySpec = new DESKeySpec(password.getBytes());
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(ALGORITHM);
            SecretKey secretKey = keyFactory.generateSecret(keySpec);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, random);
            byte[] cipherData = cipher.doFinal(password.getBytes());
            return new BASE64Encoder().encode(cipherData);
        } catch (Exception e) {
            e.printStackTrace();
            return password;
        }
    }

    /**
     * DES解密字符串
     *
     * @param password 解密密码，长度不能够小于8位
     * @return 解密后内容
     */
    public static String decrypt(String password) {
        if (password== null || password.length() < 8) {
            throw new RuntimeException("加密失败，key不能小于8位");
        }

        try {
            SecureRandom random = new SecureRandom();
            DESKeySpec keySpec = new DESKeySpec(password.getBytes());
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(ALGORITHM);
            SecretKey secretKey = keyFactory.generateSecret(keySpec);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, random);
            byte[] cipherData = cipher.doFinal(password.getBytes());
            byte[] plainData = cipher.doFinal(cipherData);
            System.out.println("plainText : " + new String(plainData));
            return  new String(plainData);
        } catch (Exception e) {
            e.printStackTrace();
            return password;
        }
    }

    public static void main(String[] args) throws Exception {
        String plainText = new String("Bjeas_2020");
        BASE64Encoder encoder = new BASE64Encoder();
        BASE64Decoder decoder = new BASE64Decoder();
        String cipherText = encoder.encode(plainText.getBytes());
        System.out.println("cipherText : " + cipherText);
        System.out.println("plainText : " + new String(decoder.decodeBuffer(cipherText)));

    }
}