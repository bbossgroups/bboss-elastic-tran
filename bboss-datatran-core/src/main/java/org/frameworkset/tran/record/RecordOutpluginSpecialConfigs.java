package org.frameworkset.tran.record;
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
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.plugin.OutputPlugin;

import java.util.*;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/1/6
 */
public class RecordOutpluginSpecialConfigs {

    private Map<String,RecordOutpluginSpecialConfig> pluginSpecialConfigs;
    public RecordOutpluginSpecialConfigs addRecordOutpluginSpecialConfig(OutputPlugin outputPlugin,RecordOutpluginSpecialConfig recordOutpluginSpecialConfig){
        if(pluginSpecialConfigs == null){
            pluginSpecialConfigs = new LinkedHashMap<>();
        }
        String pluginNo = outputPlugin.getOutputConfig().getPluginNo();
        pluginSpecialConfigs.put(pluginNo,recordOutpluginSpecialConfig);
        return this;
    }
    public RecordColumnInfo getRecordColumnInfo(OutputConfig outputConfig, String name){
        RecordOutpluginSpecialConfig recordOutpluginSpecialConfig = getRecordOutpluginSpecialConfig(outputConfig);
        return recordOutpluginSpecialConfig.getRecordColumnInfo(name);
    }
    public RecordOutpluginSpecialConfig getRecordOutpluginSpecialConfig(OutputConfig outputConfig){
        if(pluginSpecialConfigs == null)
            return null;
        String pluginNo = outputConfig.getPluginNo();
        return pluginSpecialConfigs.get(pluginNo);
    }

    public RecordOutpluginSpecialConfig getRecordOutpluginSpecialConfig(OutputPlugin outputPlugin){
        if(pluginSpecialConfigs == null)
            return null;
        String pluginNo = outputPlugin.getOutputConfig().getPluginNo();
        return pluginSpecialConfigs.get(pluginNo);
    }

    public void afterRefactor(Context context) throws Exception {
        if(pluginSpecialConfigs != null){
            Iterator<Map.Entry<String, RecordOutpluginSpecialConfig>> iterator = pluginSpecialConfigs.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, RecordOutpluginSpecialConfig> recordOutpluginSpecialConfigEntry = iterator.next();
                recordOutpluginSpecialConfigEntry.getValue().afterRefactor(  context);
            }
        }
    }

    public void addRecordSpecialConfig(String name, Object value){
        if(pluginSpecialConfigs != null){
            Iterator<Map.Entry<String, RecordOutpluginSpecialConfig>> iterator = pluginSpecialConfigs.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, RecordOutpluginSpecialConfig> recordOutpluginSpecialConfigEntry = iterator.next();
                recordOutpluginSpecialConfigEntry.getValue().addRecordSpecialConfig(name,value);
            }
        }
    }

    public void resolveRecordColumnInfo(String name,Object temp, FieldMeta fieldMeta, Context context) {

        if(pluginSpecialConfigs != null){
            Iterator<Map.Entry<String, RecordOutpluginSpecialConfig>> iterator = pluginSpecialConfigs.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, RecordOutpluginSpecialConfig> recordOutpluginSpecialConfigEntry = iterator.next();
                recordOutpluginSpecialConfigEntry.getValue().resolveRecordColumnInfo(    name,temp,   fieldMeta,   context);
            }
        }
    }
}
