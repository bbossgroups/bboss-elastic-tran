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
    protected FileImportContext fileImportContext;

    public FileResultSet(ImportContext importContext) {
        super(importContext);
        fileImportContext = (FileImportContext) importContext;
    }
    @Override
    protected Record buildRecord(Object data) {
        return (Record)data;
    }
}
