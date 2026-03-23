package org.frameworkset.tran.plugin.custom.output;
/**
 * Copyright 2020 bboss
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
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.OutputPlugin;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:48
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class SimpleCustomOutputConfig extends CustomOutputConfig implements OutputConfig<CustomOutputConfig> {

   
    private CustomOutPutV1 customOutPutV1;
     


	@Override
	public void build(ImportContext importContext,ImportBuilder importBuilder) {
        this.customOutPutV1 = getCustomOutPutV1();
        if(customOutPutV1 == null){
            throw new DataImportException("必须设置CustomOutPutV1接口处理数据");
            
        }
        
	}

	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new CustomOutputDataTranPlugin(this,importContext);
	}


    
    protected abstract void handleData(CustomOutPutContext customOutPutContext);


    public CustomOutPutV1 getCustomOutPutV1() {
        return customOutPutContext -> SimpleCustomOutputConfig.this.handleData(customOutPutContext);
    }

  
}
