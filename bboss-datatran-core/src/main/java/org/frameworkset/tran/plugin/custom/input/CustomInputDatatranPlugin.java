package org.frameworkset.tran.plugin.custom.input;
/**
 * Copyright 2026 bboss
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

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.plugin.BaseInputPlugin;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.ResourceStartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author biaoping.yin
 * @Date 2026/11/24
 */
public class CustomInputDatatranPlugin extends BaseInputPlugin {
	//	private TableFactory tableFactory;
	private static Logger logger = LoggerFactory.getLogger(CustomInputDatatranPlugin.class);
	private CustomInputConfig customInputConfig;
	
	private ResourceStartResult resourceStartResult = new ResourceStartResult();
	@Override
	public void init(){
		
		
	}
	//    @Override
//    public TranJob getTranJob(){
//        return new CommonRecordTranJob();
//    }
//	@Override
//	public Context buildContext(TaskContext taskContext, Record record, BatchContext batchContext){
//		return new HBaseRecordContextImpl(  taskContext,importContext,    record, batchContext);
//		
//	}
	
	
	public CustomInputDatatranPlugin(ImportContext importContext){
		super(  importContext);
		customInputConfig = (CustomInputConfig) importContext.getInputConfig();
		this.jobType = "CustomInputDatatranPlugin";
	}
	@Override
	public void destroy(boolean waitTranStop){
		CustomPluginDestroy customPluginDestroy = customInputConfig.getCustomPluginDestroy();
		if(  customPluginDestroy != null){
			customPluginDestroy.destroyPlugin(this.resourceStartResult,importContext);
		}
//		Map<String,Object> dbs = resourceStartResult.getResourceStartResult();
//		if (dbs != null && dbs.size() > 0){
//			Iterator<Map.Entry<String, Object>> iterator = dbs.entrySet().iterator();
//			while(iterator.hasNext()){
//				Map.Entry<String, Object> entry = iterator.next();
////				HBaseHelperFactory.destroy(entry.getKey());
//			}
//		}
		
		
	}
	@Override
	public void beforeInit() {
//		initHBase();
		CustomPluginInit customPluginInit = customInputConfig.getCustomPluginInit();
		if(customPluginInit != null ){
			customPluginInit.initPlugin(this.resourceStartResult,importContext);
		}
		
		
	}
	protected void doTran(TaskContext taskContext){
		CustomResultSet hBaseResultSet = new CustomResultSet(importContext);
		BaseDataTran baseDataTran = dataTranPlugin.createBaseDataTran(taskContext,hBaseResultSet,null,dataTranPlugin.getCurrentStatus());
		baseDataTran.initTran();
		dataTranPlugin.callTran( baseDataTran);
	}
	 
	@Override
	public void afterInit(){
	}
	
	@Override
	public void initStatusTableId() {
		if(importContext.isIncreamentImport()) {
			//计算增量记录id
			StatusTableIdInit statusTableIdInit = this.customInputConfig.getStatusTableIdInit();
			String id = null;
			if(statusTableIdInit != null){
				id = statusTableIdInit.getStatusTableId();
				if(id == null){
					id = "";
				}
				
			}
			else{
				id = "";
			}
			importContext.setStatusTableId(id.hashCode());
		}
		
	}
	
	private void exportData(TaskContext taskContext){		
		try {
			doTran(taskContext);
		}
		catch (Exception e){
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}
		 
		
		
	}
	 
	 
	 
	@Override
	public void doImportData(TaskContext taskContext)  throws DataImportException {
		
		try {
			exportData(  taskContext);
		} catch (DataImportException e) {
			throw e;
		} catch (Exception e) {
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}
		
	}
	
}
