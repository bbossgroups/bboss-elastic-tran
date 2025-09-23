package org.frameworkset.tran.jobflow;
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

/**
 * @author biaoping.yin
 * @Date 2025/9/22
 */
public class DownloadFileMetrics {
    /**
     * 下载耗时
     */
    private long elapsed;
    /**
     * 解压耗时
     */
    private long unzipElapsed;
    
    /**
     * 校验耗时
     */
    private long validateElapsed;

    /**
     * 从当前压缩文件中解压的文件数量
     */
    private int files;
    /**
     * 远程文件路径
     */
    private String remoteFilePath;
    /**
     * 本地存放路径
     */
    private String localFilePath;
    /**
     * 其他说明信息
     */
    private String message;

    public String getRemoteFilePath() {
        return remoteFilePath;
    }

    public void setRemoteFilePath(String remoteFilePath) {
        this.remoteFilePath = remoteFilePath;
    }

    public String getLocalFilePath() {
        return localFilePath;
    }

    public void setLocalFilePath(String localFilePath) {
        this.localFilePath = localFilePath;
    }

    public long getElapsed() {
        return elapsed;
    }
    public void setElapsed(long elapsed) {
        this.elapsed = elapsed;
    }
    public long getUnzipElapsed() {
        return unzipElapsed;
    }
    public void setUnzipElapsed(long unzipElapsed) {
        this.unzipElapsed = unzipElapsed;
    }

    public long getValidateElapsed() {
        return validateElapsed;
    }

    public void setValidateElapsed(long validateElapsed) {
        this.validateElapsed = validateElapsed;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * 获取从当前压缩文件中解压的文件数量
     * @return
     */
    public int getFiles() {
        return files;
    }

    /**
     * 从当前压缩文件中解压的文件数量
     * @param files
     */
    public void setFiles(int files) {
        this.files = files;
    }
}
