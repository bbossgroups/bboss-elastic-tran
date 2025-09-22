package org.frameworkset.tran.plugin.file;
/**
 * Copyright 2024 bboss
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

import org.frameworkset.tran.input.RemoteContext;

/**
 * 抽象基类，用于配置远程文件传输的相关参数。
 * 使用泛型支持链式调用返回具体子类实例。
 *
 * @param <T> 子类类型，用于支持链式调用
 * 
 * 链式调用示例：
 * MyRemoteConfig config = new MyRemoteConfig()
 *     .setBackupSuccessFiles(true)
 *     .setTransferEmptyFiles(false)
 *     .setSuccessFilesCleanInterval(120000)
 *     .setFileLiveTime(86400);
 * 
 * 多级链式调用案例：
 * MyRemoteConfig config = new MyRemoteConfig()
 *     .setBackupSuccessFiles(true)
 *         .setTransferEmptyFiles(false)
 *         .setSuccessFilesCleanInterval(120000)
 *     .setFileLiveTime(86400)
 *         .setSendFileAsyn(true)
 *         .setSendFileAsynWorkThreads(20)
 *     .setFailedFileResendInterval(600000);
 *  @author biaoping.yin
 *  @Date 2024/8/9
 */
public abstract class BaseRemoteConfig<T extends BaseRemoteConfig> {

    protected boolean backupSuccessFiles;
    protected boolean transferEmptyFiles;
    /**
     * 单位：毫秒,默认1分钟
     * 小于等于0时禁用归档功能
     */
    protected long successFilesCleanInterval = 60000;
    /**
     * 单位：秒,默认2天
     */
    protected int fileLiveTime = 86400*2;
    /**
     * 异步发送文件，默认同步发送
     * true 异步发送 false同步发送
     */
    protected boolean sendFileAsyn;
    protected int sendFileAsynWorkThreads = 10;


    private long failedFileResendInterval = 300000l;
    /**
     * 单位：毫秒,默认1分钟
     * @return
     */
    public long getSuccessFilesCleanInterval() {
        return successFilesCleanInterval;
    }





    public T setSuccessFilesCleanInterval(long successFilesCleanInterval) {
        this.successFilesCleanInterval = successFilesCleanInterval;
        return  (T)this;
    }


    public T setFailedFileResendInterval(long failedFileResendInterval) {
        this.failedFileResendInterval = failedFileResendInterval;
        return  (T)this;
    }

    public long getFailedFileResendInterval() {//300000l
        return failedFileResendInterval;
    }


    public boolean isBackupSuccessFiles() {
        return backupSuccessFiles;
    }

    public T setBackupSuccessFiles(boolean backupSuccessFiles) {
        this.backupSuccessFiles = backupSuccessFiles;
        return  (T)this;
    }

    public boolean isTransferEmptyFiles() {
        return transferEmptyFiles;
    }

    public T setTransferEmptyFiles(boolean transferEmptyFiles) {
        this.transferEmptyFiles = transferEmptyFiles;
        return  (T)this;
    }


    public int getFileLiveTime() {
        return fileLiveTime;
    }

    /**
     * 单位：秒
     * @param fileLiveTime
     * @return
     */

    public T setFileLiveTime(int fileLiveTime) {
        this.fileLiveTime = fileLiveTime;
        return (T)this;
    }
    public boolean isSendFileAsyn() {
        return sendFileAsyn;
    }
    /**
     * 设置是否异步发送文件，默认同步发送
     * true 异步发送 false同步发送
     */
    public T setSendFileAsyn(boolean sendFileAsyn) {
        this.sendFileAsyn = sendFileAsyn;
        return (T)this;
    }

    public int getSendFileAsynWorkThreads() {
        return sendFileAsynWorkThreads;
    }

    public T setSendFileAsynWorkThreads(int sendFileAsynWorkThreads) {
        this.sendFileAsynWorkThreads = sendFileAsynWorkThreads;
        return (T)this;
    }

}
