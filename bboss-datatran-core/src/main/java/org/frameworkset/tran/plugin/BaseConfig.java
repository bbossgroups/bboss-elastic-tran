package org.frameworkset.tran.plugin;
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

import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.record.FieldMappingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/21
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseConfig  extends FieldMappingManager {
    private static final Logger log_ = LoggerFactory.getLogger(BaseConfig.class);
    protected String pluginNo;
    protected OutputPlugin outputPlugin;
    protected WrapedExportResultHandler exportResultHandler;

    protected boolean multiOutputTran;
    public boolean isMultiOutputTran(){
        return multiOutputTran;
    }
    public void setMultiOutputTran(boolean multiOutputTran){
        this.multiOutputTran = multiOutputTran;
    }
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext){
		DataTranPlugin dataTranPlugin = new DataTranPluginImpl(importContext);
		return dataTranPlugin;
	}

    public WrapedExportResultHandler getExportResultHandler() {
        return exportResultHandler;
    }

    /**
     * 获取OutputConfig实际对应的OutputPlugin
     * @return
     */
    public OutputPlugin getOutputPlugin(){
        return outputPlugin;
    }

    public void setOutputPlugin(OutputPlugin outputPlugin) {
        this.outputPlugin = outputPlugin;
    }

    public boolean isSortedDefault(){
        return false;
    }
	public void afterBuild(ImportBuilder importBuilder, ImportContext importContext){

	}


    public void setPluginNo(String pluginNo) {
        this.pluginNo = pluginNo;
    }
    public void destroyExportResultHandler(){
        try {
            if(exportResultHandler != null)
                this.exportResultHandler.destroy();
        }
        catch (Throwable e){
            log_.warn("Destroy WrapedExportResultHandler failed:",e);
        }
        
    }
    public String getPluginNo() {
        return pluginNo;
    }
	public WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		DefualtExportResultHandler _exportResultHandler = new DefualtExportResultHandler(exportResultHandler,(OutputConfig) this);
        this.exportResultHandler = _exportResultHandler;
		return _exportResultHandler;
	}
	public int getMetricsAggWindow(){
		return -1;
	}
}
