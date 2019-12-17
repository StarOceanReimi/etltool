package com.limin.etltool.database.util.nameconverter;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public class CamelCaseNameConverter implements INameConverter {

    @Override
    public String rename(String name) {
        char[] array = name.toCharArray();
        StringBuilder result = new StringBuilder();
        for(int i = 0; i < array.length; i++) {
            if(i != 0 && Character.isUpperCase(array[i]))
                result.append("_");
            result.append(Character.toLowerCase(array[i]));
        }
        return result.toString();
    }
}
