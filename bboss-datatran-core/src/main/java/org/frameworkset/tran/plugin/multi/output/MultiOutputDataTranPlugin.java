package org.frameworkset.tran.plugin.multi.output;
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
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BasePlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.concurrent.ThreadPoolFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class MultiOutputDataTranPlugin extends BasePlugin implements OutputPlugin {
	private MultiOutputConfig multiOutputConfig;
    private List<OutputPlugin> outputPlugins;
	public MultiOutputDataTranPlugin(OutputConfig pluginOutputConfig, ImportContext importContext){
		super(  pluginOutputConfig, importContext);
        multiOutputConfig = (MultiOutputConfig) pluginOutputConfig;
	}

    private ExecutorService multiOutputExecutor;
    private Object multiOutputExecutorLock = new Object();
    @Override
    public String getJobType(){
        return "MultiOutputDataTranPlugin";
    }
    public List<OutputPlugin> getOutputPlugins() {
        return outputPlugins;
    }

    public ExecutorService buildmultiOutputExecutor(){
        if(multiOutputExecutor != null)
            return multiOutputExecutor;
        synchronized (multiOutputExecutorLock) {
            if(multiOutputExecutor == null) {
                if(multiOutputConfig.getOutputConfigSize() > 0)
                    multiOutputExecutor = ThreadPoolFactory.buildThreadPool("MultiOutputExecutorThread","MultiOutputExecutorThread",
                            multiOutputConfig.getOutputConfigSize(),100,
                            -1l
                            ,1000);
            }
        }
        return multiOutputExecutor;
    }
    void initOutputPlugins(ImportContext importContext){
        outputPlugins = new ArrayList<>();
        for(OutputConfig outputConfig:multiOutputConfig.getOutputConfigs()){
            outputPlugins.add(outputConfig.getOutputPlugin(importContext));
        }
    }
	@Override
	public void afterInit(){
        if(outputPlugins != null){
            for(OutputPlugin outputPlugin:outputPlugins){
                outputPlugin.afterInit();
            }
        }
	}
	@Override
	public void beforeInit(){
        if(outputPlugins != null){
            for(OutputPlugin outputPlugin:outputPlugins){
                outputPlugin.beforeInit();
            }
        }
	}
	
 
	@Override
	public void init() {
        if(outputPlugins != null){
            for(OutputPlugin outputPlugin:outputPlugins){
                outputPlugin.init();
            }
        }
	}
 
	@Override
	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, JobCountDownLatch countDownLatch, Status currentStatus){
        
        BaseCommonRecordDataTran baseCommonRecordDataTran = null;
        if(this.multiOutputConfig.getOutputRecordsFilter() == null) {
            baseCommonRecordDataTran = new MultiOutPutDataTran(taskContext, tranResultSet, importContext, countDownLatch, currentStatus);
        }
        else{
            baseCommonRecordDataTran = new FilterMultiOutPutDataTran(taskContext, tranResultSet, importContext, countDownLatch, currentStatus);
        }
        baseCommonRecordDataTran.initTran();
        return baseCommonRecordDataTran;
	}

    /**
     * 创建内部转换器
     *
     * @param baseDataTran
     * @return
     */
    @Override
    public BaseDataTran createBaseDataTran(BaseDataTran baseDataTran) {
        BaseCommonRecordDataTran baseCommonRecordDataTran = new MultiOutPutDataTran(baseDataTran);
        return baseCommonRecordDataTran;
    }
    @Override
    protected void buildRecordOutpluginSpecialConfigs(CommonRecord dataRecord, Context context) throws Exception {
        for (OutputPlugin outputPlugin:outputPlugins) {
            outputPlugin.buildRecordOutpluginSpecialConfig(dataRecord, context);       
        }
    }

    /**
     * 通知输入插件停止采集数据
     */
    @Override
    public void stopCollectData() {
        for (OutputPlugin outputPlugin:outputPlugins) {
            outputPlugin.stopCollectData();
        }
        super.stopCollectData();
    }
    @Override
    public void destroy(boolean waitTranStop) {
        for (OutputPlugin outputPlugin:outputPlugins) {
            outputPlugin.destroy(waitTranStop);
        }
    }

    public void setDataTranPlugin(DataTranPlugin dataTranPlugin) {
        this.dataTranPlugin = dataTranPlugin;
        for (OutputPlugin outputPlugin:outputPlugins) {
            outputPlugin.setDataTranPlugin(dataTranPlugin);
        }
    }
 
}
