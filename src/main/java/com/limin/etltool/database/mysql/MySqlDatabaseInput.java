package com.limin.etltool.database.mysql;

import com.google.common.base.Stopwatch;
import com.limin.etltool.core.Batch;
import com.limin.etltool.core.BatchInput;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.core.Input;
import com.limin.etltool.database.AbstractDatabaseInput;
import com.limin.etltool.database.DatabaseConfiguration;
import com.limin.etltool.database.DatabaseSource;
import com.limin.etltool.database.util.nameconverter.CamelCaseNameConverter;
import com.limin.etltool.database.util.nameconverter.NameConverter;
import lombok.Data;
import lombok.val;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.FeatureDescriptor;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public class MySqlDatabaseInput<T> extends AbstractDatabaseInput<T> {

    public MySqlDatabaseInput(String table) {
        super(table, null);
    }

    public MySqlDatabaseInput(String table, Class<T> componentType) {
        super(table, componentType);
        if(componentType != null) {
            val descs = PropertyUtils.getPropertyDescriptors(componentType);
            setColumns(Arrays.stream(descs).map(FeatureDescriptor::getDisplayName)
                    .collect(Collectors.toList()));
        }
    }

    @Data
    @NameConverter(CamelCaseNameConverter.class)
    public static class TestBean {

        private Long userId;

        private String loginKey;
    }

    public static void main(String[] args) throws EtlException {
        DatabaseConfiguration configuration = new DatabaseConfiguration();
        configuration.setUrl("jdbc:mysql://192.168.137.57:3306/user_center");
        configuration.setUsername("dangjian");
        configuration.setPassword("uNdvlOK5vMkqU6Xx15vtmEDdxzniQiuE");
        configuration.setDriverClassName("com.mysql.jdbc.Driver");
        configuration
                .attribute("serverTimezone", "Asia/Shanghai")
                .attribute("useUnicode", true)
                .attribute("characterEncoding", "utf-8")
                .attribute("useSSL", false)
                .attribute("allowMultiQueries", true);

        DatabaseSource source = new MySqlDatabaseSource(configuration);
        Input input = new MySqlDatabaseInput("co_comment");
        Collection<Map<String, Object>> col = input.readCollection(source);
    }
}
