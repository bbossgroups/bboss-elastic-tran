package org.frameworkset.tran.input.file;

import org.frameworkset.tran.AsynBaseTranResultSet;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.context.ImportContext;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/12
 */
public class FileResultSet extends AsynBaseTranResultSet {

    public FileResultSet(ImportContext importContext) {
        super(importContext);
    }
    @Override
    protected Record buildRecord(Object data) {
        return (Record)data;
    }
}
