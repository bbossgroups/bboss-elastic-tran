package org.frameworkset.tran.plugin.file.output;
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DefualtExportResultHandler;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.ftp.FtpContext;
import org.frameworkset.tran.ftp.RemoteFileValidate;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FtpFileFilter;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.output.ftp.FtpOutConfig;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.JsonRecordGenerator;
import org.frameworkset.tran.util.RecordGenerator;
import org.frameworkset.tran.util.TranUtil;

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
public class FileOutputConfig extends BaseConfig implements OutputConfig , FtpContext {
	private FtpOutConfig ftpOutConfig;
	/**
	 * 输出文件记录处理器:org.frameworkset.tran.kafka.output.fileftp.ReocordGenerator
	 */
	private RecordGenerator recordGenerator;

	/**
	 *  导出文件名称生成接口实现类型（必须指定）：org.frameworkset.tran.kafka.output.fileftp.FilenameGenerator
	 */
	private FilenameGenerator filenameGenerator;
	private String fileDir;
	private int fileWriterBuffsize ;
	private int maxFileRecordSize;
	private boolean disableftp ;

	public String getLineSeparator() {
		return lineSeparator;
	}

	public FileOutputConfig setLineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;
		return this;
	}

	private String lineSeparator;

	@Override
	public boolean deleteRemoteFile() {
		return false;
	}
	@Override
	public String getEncoding() {
		return ftpOutConfig.getEncoding();
	}

	public String getFileDir() {
		return fileDir;
	}
	public long getFailedFileResendInterval() {//300000l
		return ftpOutConfig.getFailedFileResendInterval();
	}
	public String generateFileName(TaskContext taskContext, int fileSeq){
		return getFilenameGenerator().genName(   taskContext,fileSeq);
	}
	public void generateReocord(Context taskContext, CommonRecord record, Writer builder) throws Exception{
		if(builder == null){
			builder = RecordGenerator.tranDummyWriter;
		}
		getRecordGenerator().buildRecord(  taskContext, record,  builder);
	}
	public int getFileLiveTime() {
		return ftpOutConfig.getFileLiveTime();
	}
	@Override
	public RemoteFileValidate getRemoteFileValidate() {
		return null;
	}
	public FileOutputConfig setFileDir(String fileDir) {
		this.fileDir = fileDir;
		return  this;
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
	public Boolean useEpsvWithIPv4() {
		return ftpOutConfig.isUseEpsvWithIPv4();
	}
	@Override
	public FtpConfig getFtpConfig() {
		throw new UnsupportedOperationException("getFtpConfig");
	}
	@Override
	public Boolean localActive() {
		return ftpOutConfig.isLocalActive();
	}
	@Override
	public FileConfig getFileConfig() {
		throw new UnsupportedOperationException("getFileConfig");
	}
	public RecordGenerator getRecordGenerator() {
		return recordGenerator;
	}

	@Override
	public String getRemoteFileDir() {
		return ftpOutConfig.getRemoteFileDir();
	}
	public FileOutputConfig setRecordGenerator(RecordGenerator recordGenerator) {
		this.recordGenerator = recordGenerator;
		return  this;
	}

	public FilenameGenerator getFilenameGenerator() {
		return filenameGenerator;
	}

	public FileOutputConfig setFilenameGenerator(FilenameGenerator filenameGenerator) {
		this.filenameGenerator = filenameGenerator;
		return  this;
	}

	public int getFileWriterBuffsize() {
		return fileWriterBuffsize;
	}

	public FileOutputConfig setFileWriterBuffsize(int fileWriterBuffsize) {
		this.fileWriterBuffsize = fileWriterBuffsize;
		return  this;
	}

	public int getMaxFileRecordSize() {
		return maxFileRecordSize;
	}

	public FileOutputConfig setMaxFileRecordSize(int maxFileRecordSize) {
		this.maxFileRecordSize = maxFileRecordSize;
		return  this;
	}

	public boolean isDisableftp() {
		return disableftp;
	}

	public FileOutputConfig setDisableftp(boolean disableftp) {
		this.disableftp = disableftp;
		return  this;
	}


	public FtpOutConfig getFtpOutConfig() {
		return ftpOutConfig;
	}

	public FileOutputConfig setFtpOutConfig(FtpOutConfig ftpOutConfig) {
		this.ftpOutConfig = ftpOutConfig;
		return this;
	}

	@Override
	public void build(ImportBuilder importBuilder) {
		if(ftpOutConfig == null){
			disableftp = true;
		}

//		if(getMaxFileRecordSize() == 0){//默认1万条记录一个文件
//			setMaxFileRecordSize(50000);
//		}
		if(getRecordGenerator() == null){
			setRecordGenerator(new JsonRecordGenerator());
		}
		if (SimpleStringUtil.isEmpty(lineSeparator))
			lineSeparator = TranUtil.lineSeparator;
	}

	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new FileOutputDataTranPlugin(importContext);
	}

	@Override
	public WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler<String,String>(exportResultHandler);
	}


	public String getFtpIP() {
		return ftpOutConfig.getFtpIP();
	}



	public int getFtpPort() {
		return ftpOutConfig.getFtpPort();
	}



	public String getFtpUser() {
		return ftpOutConfig.getFtpUser();
	}

	public String getFtpPassword() {
		return ftpOutConfig.getFtpPassword();
	}

	public String getFtpProtocol() {
		return ftpOutConfig.getFtpProtocol();
	}

	public String getFtpTrustmgr() {
		return ftpOutConfig.getFtpTrustmgr();
	}

	public String getFtpProxyHost() {
		return ftpOutConfig.getFtpProxyHost();
	}

	public int getFtpProxyPort() {
		return ftpOutConfig.getFtpProxyPort();
	}

	public String getFtpProxyUser() {
		return ftpOutConfig.getFtpProxyUser();
	}

	public String getFtpProxyPassword() {
		return ftpOutConfig.getFtpProxyPassword();
	}

	public boolean printHash() {
		return ftpOutConfig.isPrintHash();
	}

	public Boolean binaryTransfer() {
		return ftpOutConfig.isBinaryTransfer();
	}

	public long getKeepAliveTimeout() {
		return ftpOutConfig.getKeepAliveTimeout();
	}

	public int getControlKeepAliveReplyTimeout() {
		return ftpOutConfig.getControlKeepAliveReplyTimeout();
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

	public int getTransferProtocol(){
		return ftpOutConfig.getTransferProtocol();
	}


	public long getSuccessFilesCleanInterval(){
		return ftpOutConfig.getSuccessFilesCleanInterval();
	}


}
