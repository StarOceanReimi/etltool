package com.limin.etltool.database;

import com.limin.etltool.core.EtlException;
import com.limin.etltool.util.Exceptions;
import lombok.Data;

import java.util.Collection;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
@Data
public abstract class AbstractDbOutput<T> extends DbSupport<T> implements DbOutput<T> {

    private static final int DEFAULT_BATCH_SIZE = 1024;

    private int batchSize = DEFAULT_BATCH_SIZE;

    public AbstractDbOutput(Class<T> componentType, Database database, DatabaseAccessor accessor) {
        super(componentType, database, accessor);
        if(!accessor.accept(this))
            throw Exceptions.inform("Database Accessor does not support this source");
    }

    @Override
    public boolean writeCollection(Collection<T> dataCollection) throws EtlException {
        return false;
    }

}
