package com.limin.etltool.database.mysql;

import com.google.common.base.Stopwatch;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.core.Input;
import com.limin.etltool.database.AbstractDatabaseInput;
import com.limin.etltool.database.DatabaseConfiguration;
import com.limin.etltool.database.DatabaseSource;
import com.limin.etltool.database.util.nameconverter.CamelCaseNameConverter;
import com.limin.etltool.database.util.nameconverter.INameConverter;
import com.limin.etltool.database.util.nameconverter.NameConverter;
import lombok.Data;
import lombok.val;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.FeatureDescriptor;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public class MySqlDatabaseInput<T> extends AbstractDatabaseInput<T> {

    public MySqlDatabaseInput(String table) {
        super(null);
    }

    public MySqlDatabaseInput(String table, Class<T> componentType) {
        super(componentType);
        if(componentType != null) {
            INameConverter converter = INameConverter.getConverter(componentType);
            val descs = PropertyUtils.getPropertyDescriptors(componentType);
            val columns = Arrays.stream(descs)
                    .map(FeatureDescriptor::getDisplayName)
                    .filter(name -> !name.equals("class"))
                    .map(converter::rename)
                    .collect(Collectors.toList());
            setColumns(columns);
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

        DatabaseSource source = new MySqlDatabaseSource(configuration);
        Input<TestBean> input = new MySqlDatabaseInput<>("uc_login_log", TestBean.class);
        val sw = Stopwatch.createStarted();
        Collection<TestBean> col = input.readCollection(source);
        System.out.println(col.size());
        System.out.println(sw.stop());

    }
}
