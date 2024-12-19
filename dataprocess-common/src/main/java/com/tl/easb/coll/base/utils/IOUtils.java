package com.tl.easb.coll.base.utils;

import java.io.*;

public class IOUtils {
    public static void closeQuietly(InputStream input) {
        try {
            if (input != null)
                input.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void closeQuietly(OutputStream os) {
        try {
            if (os != null)
                os.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void closeQuietly(Reader input) {
        try {
            if (input != null)
                input.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void closeQuietly(Writer output) {
        try {
            if (output != null)
                output.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();

        }
    }
}