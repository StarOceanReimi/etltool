package com.limin.etltool.database;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.limin.etltool.util.Exceptions;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
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
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.limin.etltool.util.Exceptions.propagate;
import static com.limin.etltool.util.TemplateUtils.logFormat;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
@Data
@Slf4j
public class DatabaseConfiguration {

    private String url;

    private String username;

    private String password;

    private String driverClassName;

    private final Map<String, Object> attributes = Maps.newHashMap();

    private static final String DEFAULT_LOCATION = "classpath:database.yml";

    private static final Yaml YAML = new Yaml();

    private static final Splitter DOT_SPLITTER = Splitter.on('.').trimResults();

    private static String getProperty(Properties properties, String name) {
        String property = properties.getProperty(name);
        if(!Strings.isNullOrEmpty(property)) return property;
        Iterator<String> iter = DOT_SPLITTER.split(name).iterator();
        Object subProp = null;
        while (iter.hasNext()) {
            if(subProp == null) subProp = properties.get(iter.next());
            if(Objects.isNull(subProp)) return null;
            if(subProp instanceof Map) subProp = ((Map) subProp).get(iter.next());
            if(subProp instanceof String) return (String) subProp;
        }
        return null;
    }

    public static DatabaseConfiguration input(ClassLoader cl, String databaseName) {
        checkNotNull(cl, "classLoader cannot be null");
        checkArgument(!Strings.isNullOrEmpty(databaseName), "database name cannot be empty");
        return withClassloader(cl, "etl-db/input").databaseName(databaseName);
    }

    public static DatabaseConfiguration output(ClassLoader cl, String databaseName) {
        checkNotNull(cl, "classLoader cannot be null");
        checkArgument(!Strings.isNullOrEmpty(databaseName), "database name cannot be empty");
        return withClassloader(cl, "etl-db/output").databaseName(databaseName);
    }

    private static DatabaseConfiguration withClassloader(ClassLoader cl, String path) {
        String activeProfile = findActiveProfile(cl);
        String file = String.format("%s/db-%s.yml", path, activeProfile);
        InputStream configStream = cl.getResourceAsStream(file);
        if(configStream == null)
            throw Exceptions.inform("cannot find file: {}", file);
        Properties properties = YAML.loadAs(configStream, Properties.class);
        return new DatabaseConfiguration(properties);
    }

    public static DatabaseConfiguration withSpringApplication(String dynamicDbKey) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String activeProfile = findActiveProfile(cl);
        InputStream configStream = cl.getResourceAsStream(String.format("application-%s.yml", activeProfile));
        Properties properties = YAML.loadAs(configStream, Properties.class);

        String dynamic = ofNullable(dynamicDbKey).orElseGet(() ->
                getProperty(properties, "spring.datasource.dynamic.primary"));
        Properties adaptProperties = new Properties();
        if(!Strings.isNullOrEmpty(dynamic)) {
            adaptProperties.put("url", requireNonNull(getProperty(
                    properties, "spring.datasource.dynamic.datasource." + dynamic + ".url")));
            adaptProperties.put("username", requireNonNull(getProperty(
                    properties, "spring.datasource.dynamic.datasource." + dynamic + ".username")));
            adaptProperties.put("password", requireNonNull(getProperty(
                    properties, "spring.datasource.dynamic.datasource." + dynamic + ".password")));
            adaptProperties.put("driverClassName", requireNonNull(
                    getProperty(properties, "spring.datasource.dynamic.datasource." + dynamic + ".driver-class-name")));
        } else {
            adaptProperties.put("url", requireNonNull(
                    getProperty(properties, "spring.datasource.url")));
            adaptProperties.put("username", requireNonNull(
                    getProperty(properties, "spring.datasource.username")));
            adaptProperties.put("password", requireNonNull(
                    getProperty(properties, "spring.datasource.password")));
            adaptProperties.put("driverClassName", requireNonNull(
                    getProperty(properties, "spring.datasource.driver-class-name")));
        }
        return new DatabaseConfiguration(adaptProperties);

    }
    public static DatabaseConfiguration withSpringApplication() {
        return withSpringApplication(null);
    }

    private static String findActiveProfile(ClassLoader cl) {
        String active = System.getProperty("spring.profiles.active");
        if(!Strings.isNullOrEmpty(active)) return active;
        InputStream stream = cl.getResourceAsStream("application.yml");
        if(stream == null) return "dev";
        Properties properties = YAML.loadAs(stream, Properties.class);
        return properties.getProperty("spring.profiles.active", "dev");
    }

    public DatabaseConfiguration() {
        this("");
    }

    private void loadFromProperties(Properties properties) {
        val descs = PropertyUtils.getPropertyDescriptors(getClass());
        for(val prop : descs) {
            if(prop.getDisplayName().equals("class")) continue;
            if(prop.getDisplayName().equals("attributes")) continue;
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
            "^(?<protocal>.+)://(?<host>.+):(?<port>\\d+)/(?<database>[^?]*)\\??(?<attributes>.*)?");

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

    public static void main(String[] args) {


    }

}
