package org.frameworkset.tran.input.zipfile;
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

import org.frameworkset.tran.jobflow.DownloadFileMetrics;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;

/**
 * 根据zip文件路径获取密码
 * @author biaoping.yin
 * @Date 2025/10/10
 */
public interface ZipFilePasswordFunction {
    /**
     * 根据zip文件路径获取密码
     * @param jobFlowNodeExecuteContext 流程节点执行上下文对象
     * @param remoteFile 远程zip文件路径
     * @param localFilePath 本地zip文件路径
     * @return
     */
    String getZipFilePassword(JobFlowNodeExecuteContext jobFlowNodeExecuteContext, String remoteFile, String localFilePath);
}
