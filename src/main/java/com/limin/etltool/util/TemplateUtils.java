package com.limin.etltool.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public abstract class TemplateUtils {

    private static final Pattern logPattern = Pattern.compile("\\{(\\d*)\\}");

    public static String logFormat(String format, Object... args) {

        if(args == null || args.length == 0) return format;
        Matcher matcher = logPattern.matcher(format);
        StringBuffer result = new StringBuffer();
        int count = 0;
        while (matcher.find()) {
            String indexStr = matcher.group(1);
            Object param;
            if(Strings.isNullOrEmpty(indexStr)) {
                param = args[count];
            } else {
                int index = Integer.parseInt(indexStr);
                checkArgument(index >= 0 && index < args.length, "index error");
                param = args[Integer.parseInt(indexStr)];
            }
            count++;
            if(param != null)
                matcher.appendReplacement(result, String.valueOf(param));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    public static void main(String[] args) {

        System.out.println(logFormat("hello"));

    }

}
