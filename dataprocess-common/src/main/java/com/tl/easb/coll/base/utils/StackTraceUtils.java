package com.tl.easb.coll.base.utils;


import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * User: Jerry Huang
 * Time: 15:13:06
 */

/**
 * This class has the functions to convert Exception StackTrace to a string.
 */
public class StackTraceUtils {

    /**
     * This method takes a exception as an input argument and returns the stacktrace as a string.
     */
    public static String getStackTrace(Throwable exception) {
        if (exception != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            exception.printStackTrace(pw);
            return sw.toString();
        } else
            return "thrown exception is null.no more exception information can provide..";
    }
}
