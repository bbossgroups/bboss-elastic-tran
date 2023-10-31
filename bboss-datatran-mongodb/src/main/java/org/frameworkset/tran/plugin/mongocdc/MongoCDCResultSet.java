package org.frameworkset.tran.plugin.mongocdc;
/**
 * Copyright 2008 biaoping.yin
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

import org.frameworkset.tran.AsynBaseTranResultSet;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.context.ImportContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/8/3 12:27
 * @author biaoping.yin
 * @version 1.0
 */
public class MongoCDCResultSet extends AsynBaseTranResultSet {
    private MongoCDCInputConfig mongoCDCInputConfig;
	public MongoCDCResultSet(ImportContext importContext, MongoCDCInputConfig mongoCDCInputConfig) {
		super(importContext);
        this.mongoCDCInputConfig = mongoCDCInputConfig;
	}

	@Override
	protected Record buildRecord(Object data) {
        return (Record)data;
	}
    @Override
    public Long getLastValueTime(){
        if(record != null) {
            MongoDBCDCData mongoDBCDCData = ((MongoCDCRecord) record).getMongoDBCDCData();
            if(mongoDBCDCData != null) {
                return mongoDBCDCData.getClusterTime();
            }
        }
        return null;
    }
    @Override
    public String getStrLastValue() throws DataImportException {
        if(record != null) {
            MongoDBCDCData mongoDBCDCData = ((MongoCDCRecord) record).getMongoDBCDCData();
            if(mongoDBCDCData != null) {
               return mongoDBCDCData.getPosition();
            }
        }
        return null;
    }

}
