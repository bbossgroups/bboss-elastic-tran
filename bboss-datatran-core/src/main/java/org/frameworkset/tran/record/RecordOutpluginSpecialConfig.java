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

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/1/6
 */
public class RecordOutpluginSpecialConfig {
    private Map<String,Object> specialConfigs;
    private OutputConfig outputConfig;
    private Map<String,RecordColumnInfo> dataInfos;
    public RecordOutpluginSpecialConfig(OutputConfig outputConfig){
        this.outputConfig = outputConfig;
    }

    public OutputConfig getOutputConfig() {
        return outputConfig;
    }

    public RecordOutpluginSpecialConfig addRecordSpecialConfig(String name, Object value){
        return _addRecordSpecialConfig(  name,   value,true);
    }

    public RecordOutpluginSpecialConfig addRecordSpecialConfigOnly(String name, Object value){
        return _addRecordSpecialConfig(  name,   value,false);
    }
    private RecordOutpluginSpecialConfig _addRecordSpecialConfig(String name, Object value,boolean prehandle){
        if(prehandle)
            value = outputConfig.preHandleSpecialConfig(name,value);
        if(specialConfigs == null)
            specialConfigs = new HashMap<>();
        specialConfigs.put(name,value);
        return this;
    }
    public Map<String, Object> getSpecialConfigs() {
        return specialConfigs;
    }

    public Object getSpecialConfig(String name) {
        if(specialConfigs == null){
            return null;
        }
        return specialConfigs.get(name);
    }

    public String getSpecialStringConfig(String name) {
        if(specialConfigs == null){
            return null;
        }
        return String.valueOf(specialConfigs.get(name));
    }
 

    public void afterRefactor(Context context) throws Exception {
        outputConfig.afterRefactor(this,context);
    }
    public RecordColumnInfo getRecordColumnInfo( String name){
        if(dataInfos != null){
            return dataInfos.get(name);
        }
        return null;
    }
    public void resolveRecordColumnInfo(String name,Object temp, FieldMeta fieldMeta, Context context) {
        RecordColumnInfo recordColumnInfo = outputConfig.getOutputPlugin().resolveRecordColumnInfo(temp,   fieldMeta,   context);
        if(recordColumnInfo != null){
            if(dataInfos == null ){
                dataInfos = new LinkedHashMap<>();
            }
            dataInfos.put(name,recordColumnInfo);
        }
    }
    
    
}
