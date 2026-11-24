package org.frameworkset.tran.plugin.custom.input;
/**
 * Copyright 2026 bboss
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
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.InputPlugin;

/**
 * 自定义输入插件：用户自定义数据生成逻辑，然后将生成的数据，推送到数据处理管道
 * 需要沿用现有数据增量状态管理逻辑
 * @author biaoping.yin
 * @Date 2026/11/23
 */
public class CustomInputConfig extends BaseConfig<CustomInputConfig> implements InputConfig<CustomInputConfig> {
	
	private CustomPluginInit customPluginInit;
	private CustomPluginDestroy customPluginDestroy;
	private StatusTableIdInit statusTableIdInit;
	

	
	private CustomDataGenerator customDataGenerator ;
	
	@Override
	public void build(ImportContext importContext, ImportBuilder importBuilder) {
		if(customDataGenerator == null){
			throw new DataImportException("customDataGenerator can not be null");
		}
	}
	
	@Override
	public InputPlugin getInputPlugin(ImportContext importContext) {
		return new CustomInputDatatranPlugin(importContext);
	}
	
	/**
	 * 初始化自定义输入插件依赖的资源，譬如各种数据源等等
	 * ，在作业结束时，会对这些资源进行销毁
	 * @param customPluginInit
	 */
	public CustomInputConfig setCustomPluginInit(CustomPluginInit customPluginInit){
		this.customPluginInit = customPluginInit;
		return this;
		
	}
	/**
	 * 作业结束时，销毁初始化自定义输入插件依赖的资源，譬如各种数据源等等
	 * @param customPluginDestroy
	 */
	public CustomInputConfig setCustomPluginDestroy(CustomPluginDestroy customPluginDestroy){
		this.customPluginDestroy = customPluginDestroy;
		return this;
		
	}
	
	public CustomPluginDestroy getCustomPluginDestroy() {
		return customPluginDestroy;
	}
	public CustomPluginInit getCustomPluginInit() {
		return customPluginInit;
	}
	public CustomInputConfig setStatusTableIdInit(StatusTableIdInit statusTableIdInit) {
		this.statusTableIdInit = statusTableIdInit;
		return this;
	}
	public StatusTableIdInit getStatusTableIdInit() {
		return statusTableIdInit;
	}
	public CustomDataGenerator getCustomDataGenerator() {
		return customDataGenerator;
	}
	
	public CustomInputConfig setCustomDataGenerator(CustomDataGenerator customDataGenerator) {
		this.customDataGenerator = customDataGenerator;
		return this;
	}
}
