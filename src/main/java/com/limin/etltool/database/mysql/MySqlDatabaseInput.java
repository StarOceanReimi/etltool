package com.limin.etltool.database.mysql;

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
            val columns = Arrays.stream(descs)
                    .map(FeatureDescriptor::getDisplayName)
                    .filter(name -> !name.equals("class"))
                    .collect(Collectors.toList());
            setColumns(columns);
        }
    }

    @Data
    @NameConverter(CamelCaseNameConverter.class)
    public static class TestBean {

        private Long pid;

        private Long cid;
    }

    public static void main(String[] args) throws EtlException {
        DatabaseConfiguration configuration = new DatabaseConfiguration();
        configuration.setUrl("jdbc:mysql://localhost:3306/hierarchical_data_exec");
        configuration.setUsername("root");
        configuration.setPassword("db0723..");
        configuration.setDriverClassName("com.mysql.jdbc.Driver");
        configuration
                .attribute("serverTimezone", "Asia/Shanghai")
                .attribute("useUnicode", true)
                .attribute("characterEncoding", "utf-8")
                .attribute("useSSL", false)
                .attribute("allowMultiQueries", true);

        DatabaseSource source = new MySqlDatabaseSource(configuration);
        Input<TestBean> input = new MySqlDatabaseInput<>("relation", TestBean.class);
        Collection<TestBean> col = input.readCollection(source);
        col.forEach(System.out::println);

    }
}
