package org.frameworkset.tran.config;
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

import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.context.BaseImportContext;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/18
 * @author biaoping.yin
 * @version 1.0
 */
public interface OutputConfig {
	public void build(ImportContext importContext,ImportBuilder importBuilder);
	OutputPlugin getOutputPlugin(ImportContext importContext);
	public WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler);
	void afterBuild(ImportBuilder importBuilder,ImportContext importContext);
	public int getMetricsAggWindow();
    default public List<ETLMetrics> getMetrics() {
        return null;
    }
}
