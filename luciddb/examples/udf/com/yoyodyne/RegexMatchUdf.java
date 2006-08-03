package com.yoyodyne;

public class RegexMatchUdf
{
    public static boolean execute(String input, String pattern)
    {
        return input.matches(pattern);
    }
}
