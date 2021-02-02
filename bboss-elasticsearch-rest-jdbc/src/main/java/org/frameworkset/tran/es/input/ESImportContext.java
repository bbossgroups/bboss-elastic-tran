package org.frameworkset.tran.es.input;
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

import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.BaseImportContext;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/21 16:13
 * @author biaoping.yin
 * @version 1.0
 */
public class ESImportContext extends BaseImportContext implements ESInputContext {
	private ESImportConfig es2esImportConfig;



	protected void init(BaseImportConfig baseImportConfig){
		super.init(baseImportConfig);
		es2esImportConfig = (ESImportConfig)baseImportConfig;
	}


	public ESImportContext(){
		this(new ESImportConfig());

	}
	public ESImportContext(BaseImportConfig baseImportConfig){
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
	public QueryUrlFunction getQueryUrlFunction(){
		return es2esImportConfig.getQueryUrlFunction();
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




}
