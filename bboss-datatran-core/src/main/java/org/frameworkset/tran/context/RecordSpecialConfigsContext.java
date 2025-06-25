package org.frameworkset.tran.context;
/**
 * Copyright 2025 bboss
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

import org.frameworkset.tran.FieldMeta;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.jobflow.context.JobFlowExecuteContext;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.record.RecordOutpluginSpecialConfig;
import org.frameworkset.tran.record.RecordOutpluginSpecialConfigs;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/1/7
 */
public class RecordSpecialConfigsContext {
    private RecordOutpluginSpecialConfigs recordOutpluginSpecialConfigs;
    private RecordOutpluginSpecialConfig recordOutpluginSpecialConfig;
    private ImportContext importContext;
    private DBInputConfig dbInputConfig;
    private DBOutputConfig dbOutputConfig;
    public RecordSpecialConfigsContext(ImportContext importContext){
        this.importContext = importContext;
        importContext.getOutputConfig();
        if(importContext.getOutputConfig() instanceof DBOutputConfig){
            dbOutputConfig = (DBOutputConfig)importContext.getOutputConfig();
        }
        InputConfig inputConfig = importContext.getInputConfig();
        if(inputConfig instanceof DBInputConfig){
            dbInputConfig = (DBInputConfig)inputConfig;
        }
    }
    public RecordOutpluginSpecialConfigs getRecordOutpluginSpecialConfigs() {
        return recordOutpluginSpecialConfigs;
        
    }
    public void resolveRecordColumnInfo(String name,Object temp, FieldMeta fieldMeta, Context context){
        if(recordOutpluginSpecialConfig != null) {
            recordOutpluginSpecialConfig.resolveRecordColumnInfo(   name, temp,   fieldMeta,  context);
        }
        else if(recordOutpluginSpecialConfigs != null){
            recordOutpluginSpecialConfigs.resolveRecordColumnInfo(   name, temp,   fieldMeta,  context);
        }
        
    }
    public void afterRefactor(Context context) throws Exception {
        if(recordOutpluginSpecialConfig != null) {
            recordOutpluginSpecialConfig.afterRefactor(  context);
        }
        else if(recordOutpluginSpecialConfigs != null){
            recordOutpluginSpecialConfigs.afterRefactor(  context);
        }
    }
    
    public String getDBName(){
        if(dbInputConfig != null)
            return dbInputConfig.getDBName();
        else if(dbOutputConfig != null){
            return dbInputConfig.getDBName();
        }
        else
            return null;
    }

    public void setRecordOutpluginSpecialConfigs(RecordOutpluginSpecialConfigs recordOutpluginSpecialConfigs) {
        this.recordOutpluginSpecialConfigs = recordOutpluginSpecialConfigs;
    }

    public RecordOutpluginSpecialConfig getRecordOutpluginSpecialConfig() {
        return recordOutpluginSpecialConfig;
    }

    public void setRecordOutpluginSpecialConfig(RecordOutpluginSpecialConfig recordOutpluginSpecialConfig) {
        this.recordOutpluginSpecialConfig = recordOutpluginSpecialConfig;
    }

    public void addRecordOutpluginSpecialConfig(OutputPlugin outputPlugin, RecordOutpluginSpecialConfig recordOutpluginSpecialConfig) {
        if(recordOutpluginSpecialConfigs == null){
            recordOutpluginSpecialConfigs = new RecordOutpluginSpecialConfigs();
            
        }
        recordOutpluginSpecialConfigs.addRecordOutpluginSpecialConfig(outputPlugin,recordOutpluginSpecialConfig);
    }

    public RecordOutpluginSpecialConfig getRecordOutpluginSpecialConfig(OutputPlugin outputPlugin) {
        if(recordOutpluginSpecialConfigs == null){
           return recordOutpluginSpecialConfig;

        }
        return recordOutpluginSpecialConfigs.getRecordOutpluginSpecialConfig(outputPlugin);
    }

    public void addRecordSpecialConfig(String name, Object value){
        if(recordOutpluginSpecialConfigs == null){
            recordOutpluginSpecialConfig.addRecordSpecialConfig(  name,   value);

        }
        else if(recordOutpluginSpecialConfigs != null){
            recordOutpluginSpecialConfigs.addRecordSpecialConfig(  name,   value);
        }
    }

    /**
     * 获取工作流节点执行上下文对象
     *
     * @return
     */
    public JobFlowNodeExecuteContext getJobFlowNodeExecuteContext() {
        return importContext.getJobFlowNodeExecuteContext();
    }

    /**
     * 获取工作流执行上下文对象
     *
     * @return
     */
    public JobFlowExecuteContext getJobFlowExecuteContext() {
        return importContext.getJobFlowExecuteContext();
    }

    /**
     * 获取子节点所属的复合节点（串行/并行）执行上下文对象
     *
     * @return
     */
    public JobFlowNodeExecuteContext getContainerJobFlowNodeExecuteContext() {
        return importContext.getJobFlowNodeExecuteContext().getContainerJobFlowNodeExecuteContext();
    }
}
