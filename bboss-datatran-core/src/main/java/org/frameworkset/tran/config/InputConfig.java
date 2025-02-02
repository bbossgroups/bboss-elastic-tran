package org.frameworkset.tran.config;
/**
 * Copyright 2022 bboss
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

import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.context.BaseImportContext;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.context.RecordSpecialConfigsContext;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.record.RecordBuidler;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/18
 * @author biaoping.yin
 * @version 1.0
 */
public interface InputConfig extends ConfigId{
	public void build(ImportContext importContext,ImportBuilder importBuilder);
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext);
	InputPlugin getInputPlugin(ImportContext importContext);
	void afterBuild(ImportBuilder importBuilder,ImportContext importContext);
    default void initRecordSpecialConfigsContext(RecordSpecialConfigsContext recordSpecialConfigsContext){
    }
    default boolean isParallelDatarefactor(){
        return true;
    }
    /**
     * 并行Datarefactor处理需要设置RecordBuidler，默认为DBRecordBuilder，如果需要自定义resultset record，从DBRecordBuilder继承实现方法即可：
     * public Map<String, Object> build(RecordBuidlerContext<ResultSet> recordBuidlerContext) throws DataImportException
     * @param parallelDatarefactor
     * @return
     */
    default InputConfig setParallelDatarefactor(boolean parallelDatarefactor){
        return this;
    }
    default RecordBuidler getRecordBuidler(){
        return null;
    }
    /**
     * 并行Datarefactor处理标记为parallelDatarefactor=true时，需要设置RecordBuidler，默认为DBRecordBuilder，如果需要自定义resultset record，从DBRecordBuilder继承实现方法即可：
     * public Map<String, Object> build(RecordBuidlerContext<ResultSet> recordBuidlerContext) throws DataImportException
     * @param recordBuidler
     * @return
     */
    default InputConfig setRecordBuidler(RecordBuidler recordBuidler){
        return this;
    }
    boolean isSortedDefault();
    default boolean enableLocalDate(){
        return false;
    }
    /**
     * 设置增量状态ID生成策略，在设置jobId的情况下起作用
     * STATUSID_POLICY_JOBID 采用jobType+jobId作为增量状态id
     * STATUSID_POLICY_JOBID_QUERYSTATEMENT 采用jobType+jobId+作业查询语句hashcode或者文件名称，作为增量id作为增量状态id
     * 默认值STATUSID_POLICY_JOBID_QUERYSTATEMENT 
     */
    default Integer getStatusIdPolicy(ImportContext importContext) {
//        if(importContext.getStatusIdPolicy() != null)
//            return baseImportConfig.getStatusIdPolicy();
//        else {
//            return getInputConfig().getStatusIdPolicy();
//        }
        return importContext.getImportConfig().getStatusIdPolicy();
    }
     

}
