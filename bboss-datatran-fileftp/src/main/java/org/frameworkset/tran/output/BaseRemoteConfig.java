package org.frameworkset.tran.output;
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

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/8/9
 */
public abstract class BaseRemoteConfig {

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





    public BaseRemoteConfig setSuccessFilesCleanInterval(long successFilesCleanInterval) {
        this.successFilesCleanInterval = successFilesCleanInterval;
        return  this;
    }


    public BaseRemoteConfig setFailedFileResendInterval(long failedFileResendInterval) {
        this.failedFileResendInterval = failedFileResendInterval;
        return  this;
    }

    public long getFailedFileResendInterval() {//300000l
        return failedFileResendInterval;
    }


    public boolean isBackupSuccessFiles() {
        return backupSuccessFiles;
    }

    public BaseRemoteConfig setBackupSuccessFiles(boolean backupSuccessFiles) {
        this.backupSuccessFiles = backupSuccessFiles;
        return  this;
    }

    public boolean isTransferEmptyFiles() {
        return transferEmptyFiles;
    }

    public BaseRemoteConfig setTransferEmptyFiles(boolean transferEmptyFiles) {
        this.transferEmptyFiles = transferEmptyFiles;
        return  this;
    }


    public int getFileLiveTime() {
        return fileLiveTime;
    }

    /**
     * 单位：秒
     * @param fileLiveTime
     * @return
     */

    public BaseRemoteConfig setFileLiveTime(int fileLiveTime) {
        this.fileLiveTime = fileLiveTime;
        return this;
    }
    public boolean isSendFileAsyn() {
        return sendFileAsyn;
    }
    /**
     * 设置是否异步发送文件，默认同步发送
     * true 异步发送 false同步发送
     */
    public BaseRemoteConfig setSendFileAsyn(boolean sendFileAsyn) {
        this.sendFileAsyn = sendFileAsyn;
        return this;
    }

    public int getSendFileAsynWorkThreads() {
        return sendFileAsynWorkThreads;
    }

    public BaseRemoteConfig setSendFileAsynWorkThreads(int sendFileAsynWorkThreads) {
        this.sendFileAsynWorkThreads = sendFileAsynWorkThreads;
        return this;
    }

}
