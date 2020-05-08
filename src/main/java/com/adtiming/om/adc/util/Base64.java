package com.adtiming.om.adc.util;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class Base64 {

    public static String encode(byte[] bytes) {
        return new BASE64Encoder().encode(bytes);
    }

    public static byte[] decode(String base64Code) {

        try {
            return base64Code == null || "".equals(base64Code) ? null : new BASE64Decoder().decodeBuffer(base64Code);
        } catch (IOException e) {
            return base64Code.getBytes();
        }
    }

    public static String encode(String inputStr) throws UnsupportedEncodingException {
        byte[] textByte = inputStr.getBytes("UTF-8");
        return encode(textByte);
    }
}
