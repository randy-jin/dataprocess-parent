package com.tl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    static String regex = ".*[A-Z]+.*";

    static Pattern compile = Pattern.compile(regex);


    public static void main(String[] args) {
        Matcher zllljols = compile.matcher("zllsdslDD&ols");
        boolean matches = zllljols.matches();
        System.out.println(matches);
    }
}
