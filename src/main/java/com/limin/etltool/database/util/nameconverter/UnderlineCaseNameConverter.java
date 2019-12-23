package com.limin.etltool.database.util.nameconverter;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/23
 */
public class UnderlineCaseNameConverter implements INameConverter {

    @Override
    public String rename(String name) {
        StringBuilder builder = new StringBuilder();
        char[] array = name.toCharArray();
        for(int i=0; i<array.length; i++) {
            if(array[i] == '_')
                builder.append(Character.toUpperCase(array[++i]));
            else
                builder.append(array[i]);
        }
        return builder.toString();
    }

    public static void main(String[] args) {
        System.out.println(new UnderlineCaseNameConverter().rename("trans_time"));
    }
}
