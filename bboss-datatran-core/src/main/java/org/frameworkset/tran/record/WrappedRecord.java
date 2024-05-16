package org.frameworkset.tran.record;
/**
 * Copyright 2024 bboss
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.TranMeta;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/5/15
 */
public class WrappedRecord implements Record {
    private Record record;
    private Object keys;
    public WrappedRecord( TranResultSet tranResultSet){
        this.record = tranResultSet.getCurrentRecord();
        keys = tranResultSet.getKeys();
    }
    /**
     * 返回封装的源Record
     * @return
     */
    public Record getRecord(){
        return record;
    }

    @Override
    public Object getValue(int i, String colName, int sqlType) throws DataImportException {
        return record.getValue(i,colName,sqlType);
    }

    @Override
    public Object getValue(String colName, int sqlType) throws DataImportException {
        return record.getValue(colName,sqlType);
    }

    @Override
    public Date getDateTimeValue(String colName) throws DataImportException {
        return record.getDateTimeValue(colName);
    }

    @Override
    public LocalDateTime getLocalDateTimeValue(String colName) throws DataImportException {
        return record.getLocalDateTimeValue(colName);
    }

    @Override
    public boolean isRecordDirectIgnore() {
        return record.isRecordDirectIgnore();
    }

    @Override
    public Date getDateTimeValue(String colName, String dateformat) throws DataImportException {
        return record.getDateTimeValue(colName,dateformat);
    }

    @Override
    public Object getValue(String colName) {
        return record.getValue(colName);
    }

    @Override
    public Object getKeys() {
        return keys;
    }

    @Override
    public Object getData() {
        return record.getData();
    }

    @Override
    public Object getMetaValue(String metaName) {
        return record.getMetaValue(metaName);
    }

    @Override
    public long getOffset() {
        return record.getOffset();
    }

    @Override
    public boolean removed() {
        return record.removed();
    }

    @Override
    public boolean reachEOFClosed() {
        return record.reachEOFClosed();
    }

    @Override
    public boolean reachEOFRecord() {
        return record.reachEOFRecord();
    }

    @Override
    public TaskContext getTaskContext() {
        return record.getTaskContext();
    }

    @Override
    public ImportContext getImportContext() {
        return record.getImportContext();
    }

    @Override
    public Map<String, Object> getMetaDatas() {
        return record.getMetaDatas();
    }

    @Override
    public Map<String, Object> getUpdateFromDatas() {
        return record.getUpdateFromDatas();
    }

    @Override
    public int getAction() {
        return record.getAction();
    }

    @Override
    public LastValueWrapper getLastValueWrapper() {
        return record.getLastValueWrapper();
    }

    @Override
    public TranMeta getMetaData() {
        return record.getMetaData();
    }

    @Override
    public void setTranMeta(TranMeta tranMeta) {
        record.setTranMeta(tranMeta);
    }
}
