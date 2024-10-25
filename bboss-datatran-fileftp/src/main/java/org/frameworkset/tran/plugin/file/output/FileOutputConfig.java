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
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.ftp.FtpContext;
import org.frameworkset.tran.ftp.RemoteFileValidate;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FilelogPluginException;
import org.frameworkset.tran.input.file.FtpFileFilter;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.output.BaseRemoteConfig;
import org.frameworkset.tran.output.minio.MinioFileConfig;
import org.frameworkset.tran.output.fileftp.FileSend2Ftp;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.output.ftp.FtpOutConfig;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static Logger logger = LoggerFactory.getLogger(FileOutputConfig.class);
	private FtpOutConfig ftpOutConfig;
	private FileSend2Ftp fileSend2Ftp;
    private MinioFileConfig minioFileConfig;
    private BaseRemoteConfig baseRemoteConfig;
    
	public final static String JobExecutorDatas_genFileInfos = "jobExecutorDatas.fileFtpOutPut.genFileInfos";
	/**
	 * 输出文件记录处理器:org.frameworkset.tran.kafka.output.fileftp.ReocordGenerator
     * use recordGeneratorV1
	 */
    @Deprecated
    private RecordGenerator recordGenerator;

    private RecordGeneratorV1 recordGeneratorV1;

	/**
	 *  导出文件名称生成接口实现类型（必须指定）：org.frameworkset.tran.kafka.output.fileftp.FilenameGenerator
	 */
	private FilenameGenerator filenameGenerator;
	private String fileDir;
	private int fileWriterBuffsize ;
    private boolean splitFile;
    /**
     * 设置切割文件记录数大小
     */
    protected int maxFileRecordSize;
    /**
     * 设置切割文件空闲时间大小，如果空闲时间内没有数据到来，
     */
    protected Integer maxForceFileThreshold;



    private long maxForceFileThresholdInterval = 5000L;
	private boolean disableftp ;



    private boolean existFileReplace ;


	/**
	 * 启用作业监控metric中采集生成的文件信息清单功能
	 * 文件信息包括：本地文件路径，远程ftp文件路径（可选，启用Ftp/sftp）时有用
	 */
	private boolean enableGenFileInfoMetric;

    private String lineSeparator;

    private SendFileFunction sendFileFunction;

	public String getLineSeparator() {
		return lineSeparator;
	}

	public FileOutputConfig setLineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;
		return this;
	}

    public MinioFileConfig getMinioFileConfig() {
        return minioFileConfig;
    }

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
		return baseRemoteConfig.getFailedFileResendInterval();
	}
	public String generateFileName(TaskContext taskContext, int fileSeq){
		return getFilenameGenerator().genName(   taskContext,fileSeq);
	}
	public void generateReocord(TaskContext taskContext, TaskMetrics taskMetrics, CommonRecord record, Writer builder) throws Exception{
		if(builder == null){
			builder = RecordGeneratorV1.tranDummyWriter;
		}
        RecordGeneratorContext recordGeneratorContext = new RecordGeneratorContext();
        recordGeneratorContext.setRecord(record);
        recordGeneratorContext.setTaskContext(taskContext);
        recordGeneratorContext.setBuilder(builder);
        recordGeneratorContext.setTaskMetrics(taskMetrics).setMetricsLogAPI(taskContext.getDataTranPlugin());

        getRecordGeneratorV1().buildRecord(  recordGeneratorContext);
//		getRecordGenerator().buildRecord(  taskContext,taskMetrics, record,  builder);
	}
	public int getFileLiveTime() {
		return baseRemoteConfig.getFileLiveTime();
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
    

	@Override
	public String getRemoteFileDir() {
		return ftpOutConfig.getRemoteFileDir();
	}

    /**
     * 
     * @param recordGenerator
     * @return
     * use   public void setRecordGeneratorV1(RecordGeneratorV1 recordGeneratorV1)
     */
    @Deprecated
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
		if(ftpOutConfig == null && minioFileConfig == null){
			disableftp = true;
		}
        else if(ftpOutConfig != null && minioFileConfig != null){
            throw new FilelogPluginException("不能同时进行ftp配置和minio配置。");
        }
        else if(ftpOutConfig != null){
            baseRemoteConfig = ftpOutConfig;
            sendFileFunction = new FtpSendFileFunction(this);
            sendFileFunction.init();
        }
        else if(minioFileConfig != null){
            baseRemoteConfig = minioFileConfig;
            sendFileFunction = new MinioSendFileFunction(this);
            sendFileFunction.init();
        }

//		if(getMaxFileRecordSize() == 0){//默认1万条记录一个文件
//			setMaxFileRecordSize(50000);
//		}
		if(recordGenerator == null && recordGeneratorV1 == null){
			setRecordGeneratorV1(new JsonRecordGenerator());
		}
        if(recordGeneratorV1 == null){
            if(recordGenerator instanceof HeaderRecordGenerator) {
                recordGeneratorV1 = new DefaultHeaderRecordGeneratorV1((HeaderRecordGenerator)recordGenerator);
            }
            else{
                recordGeneratorV1 = new DefaultRecordGeneratorV1(recordGenerator);
                
            }
        }
		if (SimpleStringUtil.isEmpty(lineSeparator))
			lineSeparator = TranUtil.lineSeparator;
        if(maxFileRecordSize > 0 ){
            splitFile = true;
        }
        if(maxForceFileThreshold != null && maxForceFileThreshold > 0)
        {
            if(maxForceFileThresholdInterval <= 0L) {
                logger.info("Change maxForceFileThresholdInterval {} to default value 5000L.",maxForceFileThresholdInterval);
                this.maxForceFileThresholdInterval = 5000L;
            }
        }

	}
    
    public void shutdown(){
        if(sendFileFunction != null){
            sendFileFunction.close();
        }
    }
    

    public SendFileFunction getSendFileFunction() {
        return sendFileFunction;
    }

    @Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new FileOutputDataTranPlugin(importContext);
	}

	@Override
	public WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler<String>(exportResultHandler);
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

	public long getSocketTimeout() {
		return ftpOutConfig.getSocketTimeout();
	}

	public long getConnectTimeout() {
		return ftpOutConfig.getConnectTimeout();
	}

	public int getControlKeepAliveReplyTimeout() {
		return ftpOutConfig.getControlKeepAliveReplyTimeout();
	}

	public boolean backupSuccessFiles(){
		return baseRemoteConfig.isBackupSuccessFiles();
	}
	public boolean transferEmptyFiles(){
		if(baseRemoteConfig != null) {
			return baseRemoteConfig.isTransferEmptyFiles();
		}
		else {
			return false;
		}

	}
	public List<String> getHostKeyVerifiers(){
		return ftpOutConfig.getHostKeyVerifiers();
	}

	public int getTransferProtocol(){
		return ftpOutConfig.getTransferProtocol();
	}


	public long getSuccessFilesCleanInterval(){
		return baseRemoteConfig.getSuccessFilesCleanInterval();
	}

	/**
	 * 启用作业监控metric中采集生成的文件信息清单功能
	 * 文件信息包括：本地文件路径，远程ftp文件路径（可选，启用Ftp/sftp）时有用
	 */
	public boolean isEnableGenFileInfoMetric() {
		return enableGenFileInfoMetric;
	}
	/**
	 * 启用作业监控metric中采集生成的文件信息清单功能
	 * 文件信息包括：本地文件路径，远程ftp文件路径（可选，启用Ftp/sftp）时有用
	 * 默认false关闭，
	 */
	public FileOutputConfig setEnableGenFileInfoMetric(boolean enableGenFileInfoMetric) {
		this.enableGenFileInfoMetric = enableGenFileInfoMetric;
		return this;
	}



	public FileSend2Ftp getFileSend2Ftp() {
		return fileSend2Ftp;
	}

	public FileOutputConfig setFileSend2Ftp(FileSend2Ftp fileSend2Ftp) {
		this.fileSend2Ftp = fileSend2Ftp;
        return this;
	}

	public boolean isSendFileAsyn() {
		return baseRemoteConfig != null && baseRemoteConfig.isSendFileAsyn();



	}

    public boolean isExistFileReplace() {
        return existFileReplace;
    }

    public FileOutputConfig setExistFileReplace(boolean existFileReplace) {
        this.existFileReplace = existFileReplace;
        return this;
    }

    public Integer getMaxForceFileThreshold() {
        return maxForceFileThreshold;
    }

    public FileOutputConfig setMaxForceFileThreshold(Integer maxForceFileThreshold) {
        this.maxForceFileThreshold = maxForceFileThreshold;
        return this;
    }

    public FileOutputConfig setSplitFile(boolean splitFile) {
        this.splitFile = splitFile;
        return this;
    }

    public boolean isSplitFile() {
        return splitFile;
    }
    public long getMaxForceFileThresholdInterval() {
        return maxForceFileThresholdInterval;
    }

    public FileOutputConfig setMaxForceFileThresholdInterval(long maxForceFileThresholdInterval) {
        this.maxForceFileThresholdInterval = maxForceFileThresholdInterval;
        return this;
    }

    public FileOutputConfig setMinioFileConfig(MinioFileConfig minioFileConfig) {
        this.minioFileConfig = minioFileConfig;
        return this;
    }

    public int getSendFileAsynWorkThreads() {
        return baseRemoteConfig.getSendFileAsynWorkThreads();
    }
    
    public String getFileSendThreadName(){
        if(ftpOutConfig != null){
            return "FileSend2Ftp";
        }
        else {
            return "FileSend2Minio";
        }
    }

    public RecordGeneratorV1 getRecordGeneratorV1() {
        return recordGeneratorV1;
    }

    public void setRecordGeneratorV1(RecordGeneratorV1 recordGeneratorV1) {
        this.recordGeneratorV1 = recordGeneratorV1;
    }
}
