package org.frameworkset.tran.plugin.multi.output;
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.context.RecordSpecialConfigsContext;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/1/3
 */
public class MultiOutputConfig  extends BaseConfig implements OutputConfig {
    private static final Logger logger = LoggerFactory.getLogger(MultiOutputConfig.class);
    private List<OutputConfig> outputConfigs;
    private boolean enableMultiExportResultHandler = true;
    private int pluginNo;

    private OutputRecordsFilter outputRecordsFilter;
    
    public MultiOutputConfig addOutputConfig(OutputConfig outputConfig){
        if(outputConfig == null){
            return this;
        }
        if(outputConfig instanceof MultiOutputConfig){
            throw new DataImportException("MultiOutputConfig can't add to MultiOutputConfig!");
        }
        if(outputConfigs == null){
            outputConfigs = new ArrayList<>();
        }
        outputConfig.setPluginNo(String.valueOf(pluginNo));
        outputConfig.setMultiOutputTran(true);
        pluginNo ++;
        outputConfigs.add(outputConfig);
        return this;
    }

    public OutputRecordsFilter getOutputRecordsFilter() {
        return outputRecordsFilter;
    }

    public MultiOutputConfig setOutputRecordsFilter(OutputRecordsFilter outputRecordsFilter) {
        this.outputRecordsFilter = outputRecordsFilter;
        return this;
    }

    public List<OutputConfig> getOutputConfigs() {
        return outputConfigs;
    }

    public int getOutputConfigSize() {
        return outputConfigs != null?outputConfigs.size():0;
    }


    @Override
    public void build(ImportContext importContext, ImportBuilder importBuilder) {
        for(OutputConfig outputConfig:outputConfigs){
            outputConfig.build(importContext,importBuilder);
        }
    }

    @Override
    public OutputPlugin getOutputPlugin(ImportContext importContext) {
        MultiOutputDataTranPlugin multiOutputDataTranPlugin = new MultiOutputDataTranPlugin(this,importContext);
        multiOutputDataTranPlugin.initOutputPlugins(  importContext);
        
        return multiOutputDataTranPlugin;
    }
    @Override
    public void initRecordSpecialConfigsContext(RecordSpecialConfigsContext recordSpecialConfigsContext, boolean fromMultiOutput){
        if(fromMultiOutput){
            throw new DataImportException("MultiOutputConfig cann't be add to MultiOutputConfig plugin.");
        }
        for(OutputConfig outputConfig:outputConfigs){
            outputConfig.initRecordSpecialConfigsContext(recordSpecialConfigsContext,true);
        }
    }

    public <T extends OutputConfig> T getOutputConfig(Class<T> outputConfigClass) {
        for(OutputConfig outputConfig:outputConfigs){
            if(outputConfigClass.isInstance(outputConfig)){
                return (T)outputConfig;
            }
        }
        return null;
    }

    public <T extends OutputConfig> List<T> getOutputConfigs(Class<T> outputConfigClass) {
        List<T> ts = new ArrayList<>();
        for(OutputConfig outputConfig:outputConfigs){
            if(outputConfigClass.isInstance(outputConfig)){
                ts.add ((T)outputConfig);
            }
        }
        return ts;
    }

    public List<ETLMetrics> getMetrics() {
        List<ETLMetrics> ts = new ArrayList<>();
        for(OutputConfig outputConfig:outputConfigs){
            if(outputConfig.getMetrics() != null && outputConfig.getMetrics().size() > 0){
                ts.addAll(outputConfig.getMetrics());
            }
        }
        return ts;
    }

    @Override
    public void destroyExportResultHandler(){
        super.destroyExportResultHandler();
        if(isEnableMultiExportResultHandler()){
            for(OutputConfig outputConfig:outputConfigs){
                outputConfig.destroyExportResultHandler();
            }
        }

    }

    public boolean isEnableMultiExportResultHandler() {
        return enableMultiExportResultHandler;
    }

    public MultiOutputConfig setEnableMultiExportResultHandler(boolean enableMultiExportResultHandler) {
        this.enableMultiExportResultHandler = enableMultiExportResultHandler;
        return this;
    }

    public WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
        this.exportResultHandler = super.buildExportResultHandler(exportResultHandler);
        if(isEnableMultiExportResultHandler()){
            for(OutputConfig outputConfig:outputConfigs){
                outputConfig.buildExportResultHandler(exportResultHandler);
            }
        }
        return this.exportResultHandler;
    }
}
