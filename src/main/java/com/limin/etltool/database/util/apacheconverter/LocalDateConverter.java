package com.limin.etltool.database.util.apacheconverter;

import org.apache.commons.beanutils.Converter;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/6/29
 */
public class LocalDateConverter implements Converter {

    private String pattern;

    public LocalDateConverter() {
    }

    public LocalDateConverter(String pattern) {
        this.pattern = pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public <T> T convert(Class<T> type, Object value) {

        if(value == null)
            return null;

        if(value instanceof Timestamp)
            return (T) ((Timestamp) value).toLocalDateTime().toLocalDate();

        if(value instanceof Date)
            return (T) ((Date) value).toLocalDate();

        if(value instanceof String)
            return (T) LocalDate.parse((String) value, DateTimeFormatter.ofPattern(pattern));

        throw new UnsupportedOperationException(
                String.format("cannot convert object %s to LocalDateTime", value.getClass()));
    }
}
