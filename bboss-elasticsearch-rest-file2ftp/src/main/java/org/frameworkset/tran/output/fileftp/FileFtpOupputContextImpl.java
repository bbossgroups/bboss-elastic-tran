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
import org.frameworkset.tran.schedule.TaskContext;

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
public class FileFtpOupputContextImpl extends BaseImportContext implements FileFtpOupputContext {
	private FileFtpOupputConfig fileFtpOupputConfig;
	public FileFtpOupputContextImpl(FileFtpOupputConfig fileFtpOupputConfig){
		super(fileFtpOupputConfig);

	}
	@Override
	public void init(){
		super.init();
		this.fileFtpOupputConfig = (FileFtpOupputConfig)baseImportConfig;
		if(fileFtpOupputConfig.getReocordGenerator() == null){
			fileFtpOupputConfig.setReocordGenerator(new JsonReocordGenerator());
		}

	}
	public boolean backupSuccessFiles(){
		return fileFtpOupputConfig.isBackupSuccessFiles();
	}
	public boolean transferEmptyFiles(){
		return fileFtpOupputConfig.isTransferEmptyFiles();
	}
	public List<String> getHostKeyVerifiers(){
		return fileFtpOupputConfig.getHostKeyVerifiers();
	}
	public int getMaxFileRecordSize(){
		return fileFtpOupputConfig.getMaxFileRecordSize();
	}
	public int getTransferProtocol(){
		return fileFtpOupputConfig.getTransferProtocol();
	}
	public boolean disableftp(){
		return fileFtpOupputConfig.isDisableftp();
	}

	public long getSuccessFilesCleanInterval(){
		return fileFtpOupputConfig.getSuccessFilesCleanInterval();
	}

	public String generateFileName(TaskContext taskContext, int fileSeq){
		return fileFtpOupputConfig.getFilenameGenerator().genName(   taskContext,fileSeq);
	}
	public void generateReocord(Context taskContext, CommonRecord record, Writer builder) throws Exception{
		fileFtpOupputConfig.getReocordGenerator().buildRecord(  taskContext, record,  builder);
	}

	@Override
	public long getFailedFileResendInterval() {//300000l
		return fileFtpOupputConfig.getFailedFileResendInterval();
	}

	public int getFileWriterBuffsize(){
		return fileFtpOupputConfig.getFileWriterBuffsize();
	}

	public FilenameGenerator getFilenameGenerator() {
		return fileFtpOupputConfig.getFilenameGenerator();
	}
	public String getFileDir() {
		return fileFtpOupputConfig.getFileDir();
	}

	public ReocordGenerator getReocordGenerator() {
		return fileFtpOupputConfig.getReocordGenerator();
	}

	@Override
	public String getFtpIP() {
		return fileFtpOupputConfig.getFtpIP();
	}

	@Override
	public int getFtpPort() {
		return fileFtpOupputConfig.getFtpPort();
	}

	@Override
	public String getFtpUser() {
		return fileFtpOupputConfig.getFtpUser();
	}

	@Override
	public String getFtpPassword() {
		return fileFtpOupputConfig.getFtpPassword();
	}

	@Override
	public String getFtpProtocol() {
		return fileFtpOupputConfig.getFtpProtocol();
	}

	@Override
	public String getFtpTrustmgr() {
		return fileFtpOupputConfig.getFtpTrustmgr();
	}

	@Override
	public String getFtpProxyHost() {
		return fileFtpOupputConfig.getFtpProxyHost();
	}

	@Override
	public int getFtpProxyPort() {
		return fileFtpOupputConfig.getFtpProxyPort();
	}

	@Override
	public String getFtpProxyUser() {
		return fileFtpOupputConfig.getFtpProxyUser();
	}

	@Override
	public String getFtpProxyPassword() {
		return fileFtpOupputConfig.getFtpProxyPassword();
	}

	@Override
	public boolean printHash() {
		return fileFtpOupputConfig.isPrintHash();
	}

	@Override
	public boolean binaryTransfer() {
		return fileFtpOupputConfig.isBinaryTransfer();
	}

	@Override
	public long getKeepAliveTimeout() {
		return fileFtpOupputConfig.getKeepAliveTimeout();
	}

	@Override
	public int getControlKeepAliveReplyTimeout() {
		return fileFtpOupputConfig.getControlKeepAliveReplyTimeout();
	}

	@Override
	public String getEncoding() {
		return fileFtpOupputConfig.getEncoding();
	}

	@Override
	public String getFtpServerType() {
		return fileFtpOupputConfig.getFtpServerType();
	}

	@Override
	public boolean localActive() {
		return fileFtpOupputConfig.isLocalActive();
	}

	@Override
	public boolean useEpsvWithIPv4() {
		return fileFtpOupputConfig.isUseEpsvWithIPv4();
	}
	@Override
	public String getRemoteFileDir() {
		return fileFtpOupputConfig.getRemoteFileDir();
	}
	public int getFileLiveTime() {
		return fileFtpOupputConfig.getFileLiveTime();
	}

}
