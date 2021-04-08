package org.frameworkset.tran.output.db;
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.DBImportConfig;
import org.frameworkset.tran.db.DBImportContext;
import org.frameworkset.tran.input.file.FileBaseDataTranPlugin;
import org.frameworkset.tran.output.FileLogBaseImportBuilder;

/**
 * <p>Description: 采集日志数据并导入Database插件构建器</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/3/29 11:21
 * @author yin-bp@163.com
 * @version 1.0
 */
public class FileLog2DBImportBuilder extends FileLogBaseImportBuilder {
	public FileLog2DBImportBuilder(){
		super();
		this.message = "FileLog 2 Database Import Configs:";
	}
	@JsonIgnore
	private DBImportConfig dbmportConfig;

	@Override
	protected  FileBaseDataTranPlugin createFileBaseDataTranPlugin(ImportContext importContext, ImportContext targetImportContext){
		return new FileLog2DBDataTranPlugin(  importContext,  targetImportContext);
	}
	protected ImportContext buildTargetImportContext(BaseImportConfig targetImportConfig) {
		return new DBImportContext(targetImportConfig);
	}
	public void setOutputDBConfig(DBImportConfig dbmportConfig) {
		this.dbmportConfig = dbmportConfig;
	}
	protected void setTargetImportContext(DataStream dataStream){
		if(dbmportConfig != null)
			dataStream.setTargetImportContext(buildTargetImportContext(dbmportConfig) );
		else
			dataStream.setTargetImportContext(dataStream.getImportContext());
	}


}
