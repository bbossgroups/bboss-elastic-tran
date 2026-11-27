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

import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.Map;

/**
 *
 * @author biaoping.yin
 * @Date 2026/11/27
 */
public class GeneratorContext {
	/** 数据导入上下文，提供数据导入过程中的全局配置和运行时信息 */
	private ImportContext importContext;
	/** 自定义输入配置，定义自定义数据源输入的相关参数 */
	private CustomInputConfig customInputConfig;
	/** 任务上下文，包含当前调度任务的运行状态和元数据信息 */
	private TaskContext taskContext;
	/** 数据转换插件实例，负责数据读取和写入的具体执行 */
	private DataTranPlugin dataTranPlugin;
	/** 参数映射，传递数据生成过程中所需的额外参数 */
	private Map params;
	
	public GeneratorContext(ImportContext importContext, CustomInputConfig customInputConfig, 
							Map params,
							TaskContext taskContext, DataTranPlugin dataTranPlugin) {
		this.importContext = importContext;
		this.customInputConfig = customInputConfig;
		this.params = params;
		this.taskContext = taskContext;
		this.dataTranPlugin = dataTranPlugin;
	}
	
	public ImportContext getImportContext() {
		return importContext;
	}
	
	public CustomInputConfig getCustomInputConfig() {
		return customInputConfig;
	}
	
	public TaskContext getTaskContext() {
		return taskContext;
	}
	
	public Map getParams() {
		return params;
	}
	public DataTranPlugin getDataTranPlugin() {
		return dataTranPlugin;
	}
}
