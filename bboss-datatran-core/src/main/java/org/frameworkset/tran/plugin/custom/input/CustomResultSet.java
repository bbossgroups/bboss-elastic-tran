package org.frameworkset.tran.plugin.custom.input;
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

import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.record.NextAssert;
import org.frameworkset.tran.record.TranMetaDataLazeLoad;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/8/3 12:27
 * @author biaoping.yin
 * @version 1.0
 */
public class CustomResultSet extends LastValue implements TranResultSet {
	private  List<Map<String, Object>>  datas;
//	protected boolean stoped;
	private CustomInputConfig customInputConfig;
	private CustomDataGenerator customDataGenerator ;
	int position = 0;
	boolean first = true;

 
	
	public CustomResultSet(ImportContext importContext ) {
		this.importContext = importContext;
		customInputConfig = (CustomInputConfig) importContext.getInputConfig();
		customDataGenerator = customInputConfig.getCustomDataGenerator();
		
	}



	@Override
	public Object getValue(int i, String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
	}



	@Override
	public Object getValue(String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
	}



	private final static NextAssert nextAssert = new NextAssert();

	@Override
	public NextAssert next() throws DataImportException {
        
		try {
			nextAssert.reset();
			if(isStop() || importContext.getInputPlugin().isStopCollectData() )
				return nextAssert;
			if(first){
				Map params = dataTranPlugin.getJobInputParams(getTaskContext());
				dataTranPlugin.putLastParamValue(params);
				GeneratorContext generatorContext = new GeneratorContext(  importContext,   customInputConfig,
						params,
						getTaskContext(),   dataTranPlugin);
				datas = customDataGenerator.generateData(generatorContext);
				if(datas != null && datas.isEmpty()){
					datas = null;
				}
				first = false;
			}
			if(datas == null || datas.isEmpty())
				return nextAssert;
			Map record = null;
			if(position < datas.size()) {
				record = datas.get(position++);
			}			
			else{
				//重置数据和位置
				datas = null;
				position = 0;
				if(customDataGenerator.isPaging()) {
					Map params = dataTranPlugin.getJobInputParams(getTaskContext());
					dataTranPlugin.putLastParamValue(params);
					GeneratorContext generatorContext = new GeneratorContext(  importContext,   customInputConfig,
							params,
							getTaskContext(),   dataTranPlugin);
					datas = customDataGenerator.generateData(generatorContext);
					if (datas != null && !datas.isEmpty()) {
						record = datas.get(position++);
					}
					else{
						datas = null;
					}
				}
			}
			if( record != null){
				this.record = new CustomRecord(getTaskContext(),importContext,record,customDataGenerator);
				this.record.setTranMeta(this.getMetaData());
				nextAssert.setHasNext(true);
				return nextAssert;
			}
		} catch (Exception e) {
			throw ImportExceptionUtil.buildDataImportException(importContext.getOutputPlugin(),importContext,"Get next hbase result failed:",e);
		}

		return nextAssert;
	}

	@Override
	public TranMeta getMetaData() {
		return new DefaultTranMetaData((TranMetaDataLazeLoad)null);
	}
	public Object getKeys(){
		return  record.getKeys();
	}
	@Override
	public Object getRecord() {
		return record.getData();
	}

//	@Override
//	public void stop() {
//		stoped = true;
//	}

	@Override
	public Object getMetaValue(String fieldName) {
		return record.getMetaValue(fieldName);
	}
	 

	
}
