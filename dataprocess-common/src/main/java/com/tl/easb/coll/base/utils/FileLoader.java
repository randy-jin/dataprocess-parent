package com.tl.easb.coll.base.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class FileLoader {
    public static String load(InputStream is) {
        return inputStream2String(is);
    }

    private static String inputStream2String(InputStream is) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int i = -1;
        try {
            while ((i = is.read()) != -1) {
                baos.write(i);
            }
        } catch (IOException ie) {
            throw new RuntimeException(ie);
        } finally {
            IOUtils.closeQuietly(is);
        }
        return baos.toString();
    }
}
