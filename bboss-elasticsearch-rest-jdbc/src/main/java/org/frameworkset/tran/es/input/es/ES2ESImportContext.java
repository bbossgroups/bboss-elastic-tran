package org.frameworkset.tran.es.input.es;
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

import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.BaseImportContext;
import org.frameworkset.tran.es.input.db.ES2DBDataTranPlugin;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/21 16:13
 * @author biaoping.yin
 * @version 1.0
 */
public class ES2ESImportContext extends BaseImportContext implements ES2ESContext{
	private ES2ESImportConfig es2esImportConfig;


	protected  DataTranPlugin buildDataTranPlugin(){
		return new ES2ESDataTranPlugin(this);
	}
	protected void init(BaseImportConfig baseImportConfig){
		super.init(baseImportConfig);
		es2esImportConfig = (ES2ESImportConfig)baseImportConfig;
	}
	public ES2ESImportContext(){
		this(new ES2ESImportConfig());

	}
	public ES2ESImportContext(BaseImportConfig baseImportConfig){
		super(baseImportConfig);

	}


	@Override
	public Map getParams() {
		return es2esImportConfig.getParams();
	}

	@Override
	public boolean isSliceQuery() {
		return es2esImportConfig.isSliceQuery();
	}

	@Override
	public int getSliceSize() {
		return es2esImportConfig.getSliceSize();
	}



	@Override
	public String getQueryUrl() {
		return es2esImportConfig.getQueryUrl();
	}

	@Override
	public String getDslName() {
		return es2esImportConfig.getDslName();
	}

	@Override
	public String getScrollLiveTime() {
		return es2esImportConfig.getScrollLiveTime();
	}

	@Override
	public String getDslFile() {
		return es2esImportConfig.getDsl2ndSqlFile();
	}

	@Override
	public String getTargetElasticsearch() {
		return es2esImportConfig.getTargetElasticsearch();
	}

	public String getTargetIndexType() {
		return es2esImportConfig.getTargetIndexType();
	}


	public String getTargetIndex() {
		return es2esImportConfig.getTargetIndex();
	}

}
