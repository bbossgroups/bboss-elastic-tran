package org.frameworkset.tran.input.file;
/**
 * Copyright 2023 bboss
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

import org.frameworkset.tran.context.ImportContext;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/8/2
 * @author biaoping.yin
 * @version 1.0
 */
public class RecordExtractor<T> {
    private File file;
    private List<Map> records;
    private T dataObject;
    private ImportContext importContext;
    public RecordExtractor(T dataObject,ImportContext importContext,File file){
        this.dataObject = dataObject;
        this.importContext = importContext;
        this.file = file;
    }

    public File getFile() {
        return file;
    }

    public RecordExtractor addRecord(Map record){
        if(records == null){
            records = new ArrayList<>();
        }
        records.add(record);
        return this;
    }

    public List<Map> getRecords() {
        return records;
    }


    public T getDataObject() {
        return dataObject;
    }

    public ImportContext getImportContext() {
        return importContext;
    }
}
