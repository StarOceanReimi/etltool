package com.limin.etltool.database.util.apacheconverter;

import org.apache.commons.beanutils.Converter;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/6/29
 */
public class LocalDatetimeConverter implements Converter {

    private String pattern;

    public LocalDatetimeConverter() {

    }

    public LocalDatetimeConverter(String pattern) {
        this.pattern = pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public <T> T convert(Class<T> type, Object value) {

        if(value == null)
            return null;

        if(value instanceof Timestamp)
            return (T) ((Timestamp) value).toLocalDateTime();

        if(value instanceof Date)
            return (T) ((Date) value).toLocalDate().atStartOfDay();

        if(value instanceof Time)
            return (T) LocalDate.now().atTime(((Time) value).toLocalTime());

        if (value instanceof String)
            return (T) LocalDateTime.parse((String) value, DateTimeFormatter.ofPattern(pattern));

        throw new UnsupportedOperationException(
                String.format("cannot convert object %s to LocalDateTime", value.getClass()));
    }
}
