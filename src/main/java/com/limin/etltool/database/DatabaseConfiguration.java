package com.limin.etltool.database;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.val;
import org.apache.commons.beanutils.PropertyUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.limin.etltool.util.Exceptions.propagate;
import static com.limin.etltool.util.TemplateUtils.logFormat;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
@Data
public class DatabaseConfiguration {

    private String url;

    private String username;

    private String password;

    private String driverClassName;

    private Map<String, Object> attributes = Maps.newHashMap();

    private static final String DEFAULT_LOCATION = "classpath:database.yml";

    private static final Yaml YAML = new Yaml();

    public DatabaseConfiguration() {
        this("");
    }

    private void loadFromProperties(Properties properties) {
        val descs = PropertyUtils.getPropertyDescriptors(getClass());
        for(val prop : descs) {
            if(prop.getDisplayName().equals("class")) continue;
            Object propVal = properties.get(prop.getDisplayName());
            Method writer = prop.getWriteMethod();
            try {
                writer.invoke(this, propVal);
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    public DatabaseConfiguration(Properties properties) {
        loadFromProperties(properties);
    }

    public DatabaseConfiguration(String propertyFileLocation) {

        InputStreamReader reader;

        propertyFileLocation = Strings.isNullOrEmpty(propertyFileLocation)
                ? DEFAULT_LOCATION : propertyFileLocation;

        if(propertyFileLocation.startsWith("classpath:")) {

            propertyFileLocation = propertyFileLocation.substring("classpath:".length());
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            InputStream stream = cl.getResourceAsStream(propertyFileLocation);
            checkArgument(stream != null, logFormat("{} has no resource", propertyFileLocation));
            reader = new InputStreamReader(stream, Charsets.UTF_8);

        } else {

            if(propertyFileLocation.startsWith("file:")) {
                propertyFileLocation = propertyFileLocation.substring("file:".length());
            }
            try {
                InputStream stream = Files.newInputStream(Paths.get(propertyFileLocation), StandardOpenOption.READ);
                reader = new InputStreamReader(stream);
            } catch (IOException e) {
                throw propagate(e);
            }

        }

        Properties properties = YAML.loadAs(reader, Properties.class);
        loadFromProperties(properties);

    }

    private static final Pattern DB_URL_PATTERN = Pattern.compile(
            "^(?<protocal>.*)://(?<host>.*):(?<port>.*)/(?<database>[^?]*)\\??(?<attributes>.*)?");

    private String databaseName;

    public void setUrl(String url) {
        this.url = url;
        Matcher matcher = DB_URL_PATTERN.matcher(url);
        if(matcher.find())
            databaseName = matcher.group("database");
    }

    public void setDatabaseName(String databaseName) {
        databaseName(databaseName);
    }

    public DatabaseConfiguration databaseName(String dbName) {
        databaseName = dbName;
        if(Strings.isNullOrEmpty(url)) return this;
        Matcher matcher = DB_URL_PATTERN.matcher(url);
        if(matcher.find())
            url = replace(url, dbName, matcher, "database");
        return this;
    }

    public DatabaseConfiguration attribute(String name, Object value) {
        attributes.put(name, value);
        return this;
    }

    private static String replace(String ori, String replacement, Matcher matcher, String groupName) {
        StringBuilder builder = new StringBuilder();
        builder.append(ori, 0, matcher.start(groupName));
        builder.append(replacement);
        builder.append(ori.substring(matcher.end(groupName)));
        return builder.toString();
    }
}
