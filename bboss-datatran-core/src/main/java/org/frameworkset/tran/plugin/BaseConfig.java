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
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.record.FieldMappingManager;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/21
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseConfig  extends FieldMappingManager {
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext){
		DataTranPlugin dataTranPlugin = new DataTranPluginImpl(importContext);
		return dataTranPlugin;
	}
    public boolean isSortedDefault(){
        return false;
    }
	public void afterBuild(ImportBuilder importBuilder, ImportContext importContext){

	}
	public WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		DefualtExportResultHandler db2ESExportResultHandler = new DefualtExportResultHandler(exportResultHandler);
		return db2ESExportResultHandler;
	}
	public int getMetricsAggWindow(){
		return -1;
	}
}
