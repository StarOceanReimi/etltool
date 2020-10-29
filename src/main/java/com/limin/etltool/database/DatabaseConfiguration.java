package com.limin.etltool.database;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import com.limin.etltool.util.Exceptions;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.beanutils.PropertyUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.limin.etltool.util.Exceptions.propagate;
import static com.limin.etltool.util.TemplateUtils.logFormat;
import static java.nio.charset.StandardCharsets.UTF_8;
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

    private Map<String, Object> attributes = Maps.newHashMap();

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

    public static DatabaseConfiguration profileDir(ClassLoader cl, String dir, String databaseName) {
        checkNotNull(cl, "classLoader cannot be null");
        checkArgument(!Strings.isNullOrEmpty(dir), "profile dir cannot be empty");
        checkArgument(!Strings.isNullOrEmpty(databaseName), "database name cannot be empty");
        return withClassloader(cl, dir).databaseName(databaseName);
    }

    public static DatabaseConfiguration input(ClassLoader cl, String databaseName) {
        checkNotNull(cl, "classLoader cannot be null");
        checkArgument(!Strings.isNullOrEmpty(databaseName), "database name cannot be empty");
        return withClassloader(cl, "etl-db/input").databaseName(databaseName);
    }

    public static DatabaseConfiguration output(ClassLoader cl) {
        checkNotNull(cl, "classLoader cannot be null");
        return withClassloader(cl, "etl-db/output");
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

    private static volatile Set<String> ENV_PROFILES;

    private static final Splitter COMMA_SPLITTER = Splitter.on(",").trimResults();

    private static String findEnvProfile(ClassLoader cl, String profiles) {
        if(ENV_PROFILES == null) {
            synchronized (DatabaseConfiguration.class) {
                if(ENV_PROFILES == null) {
                    ENV_PROFILES = initEnvProfiles(cl);
                }
            }
        }
        return Sets.intersection(Sets.newHashSet(COMMA_SPLITTER.split(profiles)), ENV_PROFILES).iterator().next();
    }

    private final static ImmutableSet<String> PROFILE_FALLBACK = ImmutableSet.of("dev", "prepro", "test", "prod", "yichun");

    private static Set<String> initEnvProfiles(ClassLoader cl) {
        InputStream profileInp = cl.getResourceAsStream("profiles");
        if(profileInp == null) return PROFILE_FALLBACK;
        try (Reader reader = new InputStreamReader(profileInp, UTF_8)) {
            StringBuilder result = new StringBuilder();
            CharStreams.copy(reader, result);
            return ImmutableSet.copyOf(COMMA_SPLITTER.split(result.toString()));
        } catch (IOException e) {
            log.warn("env file read failed. cause: {}", e.getMessage());
            return PROFILE_FALLBACK;
        }
    }

    private static String findActiveProfile(ClassLoader cl) {
        String active = System.getenv("SPRING_PROFILES_ACTIVE");
        log.debug("env spring profiles active: {}", active);
        if(!Strings.isNullOrEmpty(active)) return findEnvProfile(cl, active);
        active = System.getProperty("spring.profiles.active");
        log.debug("system spring profiles active: {}", active);
        if(!Strings.isNullOrEmpty(active)) return findEnvProfile(cl, active);
        InputStream stream = cl.getResourceAsStream("application.yml");
        if(stream != null) {
            log.debug("classloader found application.yml stream");
            Properties properties = YAML.loadAs(stream, Properties.class);
            String profile = getProperty(properties, "spring.profiles.active");
            log.debug("spring application yml profile: {}", profile);
            return ofNullable(profile).map(p -> findEnvProfile(cl, p)).orElse("prod");
        }
        stream = cl.getResourceAsStream("bootstrap.yml");
        if(stream != null) {
            log.debug("classloader found bootstrap.yml stream");
            Properties properties = YAML.loadAs(stream, Properties.class);
            String profile = getProperty(properties, "spring.profiles.active");
            log.debug("spring bootstrap yml profile: {}", profile);
            return ofNullable(profile).map(p -> findEnvProfile(cl, p)).orElse("prod");
        }
        return "prod";
    }

    public DatabaseConfiguration() {
        this("");
    }

    private void loadFromProperties(Properties properties) {
        if(properties.isEmpty()) return;
        val descs = PropertyUtils.getPropertyDescriptors(getClass());
        for(val prop : descs) {
            if(prop.getDisplayName().equals("class")) continue;
            Object propVal = properties.get(prop.getDisplayName());
            if(Objects.isNull(propVal)) continue;
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

    public static DatabaseConfiguration newInstance() {
        return new DatabaseConfiguration(new Properties());
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
        if(matcher.find()) {
            databaseName = matcher.group("database");
            String attributes = matcher.group("attributes");
            if(!Strings.isNullOrEmpty(attributes)) {
                Map<String, String> atts = Splitter.on("&")
                        .trimResults().withKeyValueSeparator("=").split(attributes);
                this.attributes.putAll(atts);
                this.url = url.substring(0, url.indexOf('?'));
            }
        }
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
