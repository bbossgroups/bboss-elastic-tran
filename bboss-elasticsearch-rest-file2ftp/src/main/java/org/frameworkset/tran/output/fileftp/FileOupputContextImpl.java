package org.frameworkset.tran.output.fileftp;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.context.BaseImportContext;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FtpFileFilter;
import org.frameworkset.tran.output.ftp.FtpOutConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.JsonRecordGenerator;
import org.frameworkset.tran.util.RecordGenerator;

import java.io.Writer;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:48
 * @author biaoping.yin
 * @version 1.0
 */
public class FileOupputContextImpl extends BaseImportContext implements FileOupputContext {
	public FileOupputConfig getFileOupputConfig() {
		return fileOupputConfig;
	}

	private FileOupputConfig fileOupputConfig;
	private FtpOutConfig ftpOutConfig;
	public FileOupputContextImpl(FileOupputConfig fileOupputConfig){
		super(fileOupputConfig);

	}
	@Override
	public void init(){
		super.init();
		this.fileOupputConfig = (FileOupputConfig)baseImportConfig;
		this.ftpOutConfig = this.fileOupputConfig.getFtpOutConfig();
		if(fileOupputConfig.getRecordGenerator() == null){
			fileOupputConfig.setRecordGenerator(new JsonRecordGenerator());
		}

	}
	public FtpOutConfig getFtpOutConfig(){
		return fileOupputConfig.getFtpOutConfig();
	}
	public boolean backupSuccessFiles(){
		return ftpOutConfig.isBackupSuccessFiles();
	}
	public boolean transferEmptyFiles(){
		return ftpOutConfig.isTransferEmptyFiles();
	}
	public List<String> getHostKeyVerifiers(){
		return ftpOutConfig.getHostKeyVerifiers();
	}
	public int getMaxFileRecordSize(){
		return fileOupputConfig.getMaxFileRecordSize();
	}
	public int getTransferProtocol(){
		return ftpOutConfig.getTransferProtocol();
	}
	public boolean disableftp(){
		return fileOupputConfig.isDisableftp() || fileOupputConfig.getFtpOutConfig() == null;
	}

	public long getSuccessFilesCleanInterval(){
		return ftpOutConfig.getSuccessFilesCleanInterval();
	}

	public String generateFileName(TaskContext taskContext, int fileSeq){
		return fileOupputConfig.getFilenameGenerator().genName(   taskContext,fileSeq);
	}
	public void generateReocord(Context taskContext, CommonRecord record, Writer builder) throws Exception{
		if(builder == null){
			builder = RecordGenerator.tranDummyWriter;
		}
		getRecordGenerator().buildRecord(  taskContext, record,  builder);
	}

	@Override
	public long getFailedFileResendInterval() {//300000l
		return ftpOutConfig.getFailedFileResendInterval();
	}

	public int getFileWriterBuffsize(){
		return fileOupputConfig.getFileWriterBuffsize();
	}

	public FilenameGenerator getFilenameGenerator() {
		return fileOupputConfig.getFilenameGenerator();
	}
	public String getFileDir() {
		return fileOupputConfig.getFileDir();
	}



	@Override
	public String getFtpIP() {
		return ftpOutConfig.getFtpIP();
	}

	@Override
	public int getFtpPort() {
		return ftpOutConfig.getFtpPort();
	}

	@Override
	public FtpConfig getFtpConfig() {
		throw new UnsupportedOperationException("getFtpConfig");
	}

	@Override
	public FileConfig getFileConfig() {
		throw new UnsupportedOperationException("getFileConfig");
	}

	@Override
	public String getFtpUser() {
		return ftpOutConfig.getFtpUser();
	}

	@Override
	public String getFtpPassword() {
		return ftpOutConfig.getFtpPassword();
	}

	@Override
	public String getFtpProtocol() {
		return ftpOutConfig.getFtpProtocol();
	}

	@Override
	public String getFtpTrustmgr() {
		return ftpOutConfig.getFtpTrustmgr();
	}

	@Override
	public String getFtpProxyHost() {
		return ftpOutConfig.getFtpProxyHost();
	}

	@Override
	public int getFtpProxyPort() {
		return ftpOutConfig.getFtpProxyPort();
	}

	@Override
	public String getFtpProxyUser() {
		return ftpOutConfig.getFtpProxyUser();
	}

	@Override
	public String getFtpProxyPassword() {
		return ftpOutConfig.getFtpProxyPassword();
	}

	@Override
	public boolean printHash() {
		return ftpOutConfig.isPrintHash();
	}

	@Override
	public boolean binaryTransfer() {
		return ftpOutConfig.isBinaryTransfer();
	}

	@Override
	public long getKeepAliveTimeout() {
		return ftpOutConfig.getKeepAliveTimeout();
	}

	@Override
	public int getControlKeepAliveReplyTimeout() {
		return ftpOutConfig.getControlKeepAliveReplyTimeout();
	}

	@Override
	public FileFilter getFileFilter() {
		throw new UnsupportedOperationException("getFileFilter");
	}

	@Override
	public FtpFileFilter getFtpFileFilter() {
		throw new UnsupportedOperationException("getFtpFileFilter");
	}

	@Override
	public String getEncoding() {
		return ftpOutConfig.getEncoding();
	}

	@Override
	public String getFtpServerType() {
		return ftpOutConfig.getFtpServerType();
	}

	@Override
	public boolean localActive() {
		return ftpOutConfig.isLocalActive();
	}

	@Override
	public boolean useEpsvWithIPv4() {
		return ftpOutConfig.isUseEpsvWithIPv4();
	}
	@Override
	public String getRemoteFileDir() {
		return ftpOutConfig.getRemoteFileDir();
	}

	@Override
	public boolean deleteRemoteFile() {
		return false;
	}
	@Override
	public int getFileLiveTime() {
		return ftpOutConfig.getFileLiveTime();
	}

	@Override
	public RecordGenerator getRecordGenerator() {
		return fileOupputConfig.getRecordGenerator();
	}

}
