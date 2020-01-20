package com.limin.etltool.util;


import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.Converter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public interface JavaTimeConverters {

    String DATE_FORMAT = "yyyy-MM-dd";
    String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    Converter LocalDateTimeInstance = new Converter() {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);

        @SuppressWarnings("unchecked")
        @Override
        public <T> T convert(Class<T> type, Object value) {
            if(value instanceof String) {
                return (T) LocalDateTime.parse((String) value, formatter);
            }
            return null;
        }
    };

    Converter LocalDateInstance = new Converter() {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT);

        @SuppressWarnings("unchecked")
        @Override
        public <T> T convert(Class<T> type, Object value) {
            if(value instanceof String) {
                return (T) LocalDate.parse((String) value, formatter);
            }
            return null;
        }
    };
}
