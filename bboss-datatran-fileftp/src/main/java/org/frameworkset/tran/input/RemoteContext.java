package org.frameworkset.tran.input;
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.ftp.RemoteFileValidate;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.RemoteFileChannel;
import org.frameworkset.tran.input.zipfile.ZipFilePasswordFunction;
import org.frameworkset.tran.plugin.file.BaseRemoteConfig;

/**
 * @author biaoping.yin
 * @Date 2025/8/9
 */
public abstract class RemoteContext<T extends RemoteContext> extends BaseRemoteConfig {
    protected FileConfig fileConfig;

    protected String remoteFileDir;
    protected RemoteFileValidate remoteFileValidate;
    protected 	String downloadTempDir;
    /**
     * 工作流节点需要进行配置:设置本地文件存放路径
     */
    protected String sourcePath;
    protected String zipFilePassward;
    protected ZipFilePasswordFunction zipFilePasswordFunction;
    
    /**
     * 是否下载替换已经存在的文件
     */
    protected boolean replaceExistFile;
    
    /**
     * 是否解压文件
     */
    protected boolean unzip ;



    /**
     * 是否解压tar.gz文件
     */
    protected boolean untar ;
    /**
     * 文件解压目录
     */
    protected String unzipDir;
    /**
     * 解压后是否删除zip文件
     */
    protected boolean deleteZipFileAfterUnzip = true;
    protected RemoteFileChannel remoteFileChannel;



    protected boolean deleteRemoteFile;
    public FileConfig getFileConfig() {
        return fileConfig;
    }
 
 
    
    protected void initJob(){
//        if(downloadedFileRecord == null){
//            downloadedFileRecord = new DefaultDownloadedFileRecord();
//        }
    }
    /**
     * 解压后是否删除zip文件，默认为true
     * @param deleteZipFileAfterUnzip
     * @return
     */
    public T setDeleteZipFileAfterUnzip(boolean deleteZipFileAfterUnzip) {
        this.deleteZipFileAfterUnzip = deleteZipFileAfterUnzip;
        return (T)this;
    }

    public boolean isDeleteZipFileAfterUnzip() {
        return deleteZipFileAfterUnzip;
    }

    /**
     * 工作流节点需要进行配置:设置本地文件存放路径
     * @return
     */
    public String getSourcePath() {
        return sourcePath;
    }

    public String getUnzipDir() {
        return unzipDir;
    }

    public T setUnzipDir(String unzipDir) {
        this.unzipDir = unzipDir;
        return (T)this;
    }

    public boolean isDeleteRemoteFile() {
        return deleteRemoteFile;
    }

    public T setUnzip(boolean unzip) {
        this.unzip = unzip;
        return (T)this;
    }

    public boolean isUnzip() {
        return unzip;
    }

    public String getZipFilePassward() {
        return zipFilePassward;
    }

    public T setZipFilePasswordFunction(ZipFilePasswordFunction zipFilePasswordFunction) {
        this.zipFilePasswordFunction = zipFilePasswordFunction;
        return (T)this;
    }

    public ZipFilePasswordFunction getZipFilePasswordFunction() {
        return zipFilePasswordFunction;
    }

    public T setZipFilePassward(String zipFilePassward) {
        this.zipFilePassward = zipFilePassward;
        return (T)this;
    }

    public String getRemoteFileDir() {
        return remoteFileDir;
    }

    public T setRemoteFileDir(String remoteFileDir) {
        this.remoteFileDir = remoteFileDir;
        return (T)this;
    }

    public T setDeleteRemoteFile(boolean deleteRemoteFile) {
        this.deleteRemoteFile = deleteRemoteFile;
        return (T)this;
    }
   

    public T setRemoteFileChannel(RemoteFileChannel remoteFileChannel) {
        this.remoteFileChannel = remoteFileChannel;
        return (T)this;
    }

    public RemoteFileChannel getRemoteFileChannel() {
        return remoteFileChannel;
    }

    public T setFileConfig(FileConfig fileConfig) {
        this.fileConfig = fileConfig;
        if(SimpleStringUtil.isEmpty(sourcePath)) {
            this.setSourcePath(fileConfig.getSourcePath());
        }
        return (T)this;
    }
    
    public T setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
        return (T)this;
    }

    public RemoteFileValidate getRemoteFileValidate() {
        return remoteFileValidate;
    }

    public T setRemoteFileValidate(RemoteFileValidate remoteFileValidate) {
        this.remoteFileValidate = remoteFileValidate;
        return (T)this;
    }
 

    public String getDownloadTempDir() {
        return downloadTempDir;
    }

    public T setDownloadTempDir(String downloadTempDir) {
        this.downloadTempDir = downloadTempDir;
        return (T)this;
    }

    public boolean isUntar() {
        return untar;
    }

    public T setUntar(boolean untar) {
        this.untar = untar;
        return (T)this;
    }
    /**
     * 是否下载替换已经存在的文件
     */
    public T setReplaceExistFile(boolean replaceExistFile) {
        this.replaceExistFile = replaceExistFile;
        return (T)this;
    }
    /**
     * 是否下载替换已经存在的文件
     */
    public boolean isReplaceExistFile() {
        return replaceExistFile;
    }

    public void destroy() {
        if(remoteFileChannel != null){
            remoteFileChannel.destroy();
        }
    }
}
