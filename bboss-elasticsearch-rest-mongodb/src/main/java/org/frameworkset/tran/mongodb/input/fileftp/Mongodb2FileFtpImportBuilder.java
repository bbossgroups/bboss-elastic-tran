package org.frameworkset.tran.mongodb.input.fileftp;/*
 *  Copyright 2008 biaoping.yin
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.mongodb.MongoDBExportBuilder;
import org.frameworkset.tran.output.fileftp.FileOupputConfig;
import org.frameworkset.tran.output.fileftp.FileOupputContextImpl;

public class Mongodb2FileFtpImportBuilder   extends MongoDBExportBuilder {

	@JsonIgnore
	private FileOupputConfig fileOupputConfig;
	public Mongodb2FileFtpImportBuilder(){

	}
	public Mongodb2FileFtpImportBuilder setFileOupputConfig(FileOupputConfig fileOupputConfig) {
		this.fileOupputConfig = fileOupputConfig;
		return this;
	}
	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		return new Mongodb2FileFtpDataTranPlugin(  importContext,  targetImportContext);
	}


	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler<Object,Object>(exportResultHandler);
	}



	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		FileOupputContextImpl fileFtpOupputContext = new FileOupputContextImpl(fileOupputConfig);
		fileFtpOupputContext.init();
		return fileFtpOupputContext;
	}




	protected void setTargetImportContext(DataStream dataStream){
		if(fileOupputConfig != null)
			dataStream.setTargetImportContext(buildTargetImportContext(fileOupputConfig) );
		else
			throw new DataImportException("DummyOupputConfig is null,please set it as:\n" +
					"\t\tString ftpIp = CommonLauncher.getProperty(\"ftpIP\",\"10.13.6.127\");//同时指定了默认值\n" +
					"\t\tFileFtpOupputConfig fileFtpOupputConfig = new FileFtpOupputConfig();\n" +
					"\n" +
					"\t\tfileFtpOupputConfig.setFtpIP(ftpIp);\n" +
					"\t\tfileFtpOupputConfig.setFileDir(\"D:\\\\workdir\");\n" +
					"\t\tfileFtpOupputConfig.setFtpPort(5322);\n" +
					"\t\tfileFtpOupputConfig.addHostKeyVerifier(\"2a:da:5a:6a:cf:7d:65:e5:ac:ff:d3:73:7f:2c:55:c9\");\n" +
					"\t\tfileFtpOupputConfig.setFtpUser(\"ecs\");\n" +
					"\t\tfileFtpOupputConfig.setFtpPassword(\"ecs@123\");\n" +
					"\t\tfileFtpOupputConfig.setRemoteFileDir(\"/home/ecs/failLog\");\n" +
					"\t\tfileFtpOupputConfig.setKeepAliveTimeout(100000);\n" +
					"\t\tfileFtpOupputConfig.setTransferEmptyFiles(true);\n" +
					"\t\tfileFtpOupputConfig.setFailedFileResendInterval(-1);\n" +
					"\t\tfileFtpOupputConfig.setBackupSuccessFiles(true);\n" +
					"\n" +
					"\t\tfileFtpOupputConfig.setSuccessFilesCleanInterval(5000);\n" +
					"\t\tfileFtpOupputConfig.setFileLiveTime(86400);//设置上传成功文件备份保留时间，默认2天\n" +
					"\t\tfileFtpOupputConfig.setMaxFileRecordSize(1000);//每千条记录生成一个文件\n" +
					"\t\tfileFtpOupputConfig.setDisableftp(false);//false 启用sftp/ftp上传功能,true 禁止（只生成数据文件，保留在FileDir对应的目录下面）\n" +
					"\t\t//自定义文件名称\n" +
					"\t\tfileFtpOupputConfig.setFilenameGenerator(new FilenameGenerator() {\n" +
					"\t\t\t@Override\n" +
					"\t\t\tpublic String genName( TaskContext taskContext,int fileSeq) {\n" +
					"\t\t\t\t//fileSeq为切割文件时的文件递增序号\n" +
					"\t\t\t\tString time = (String)taskContext.getTaskData(\"time\");//从任务上下文中获取本次任务执行前设置时间戳\n" +
					"\t\t\t\tString _fileSeq = fileSeq+\"\";\n" +
					"\t\t\t\tint t = 6 - _fileSeq.length();\n" +
					"\t\t\t\tif(t > 0){\n" +
					"\t\t\t\t\tString tmp = \"\";\n" +
					"\t\t\t\t\tfor(int i = 0; i < t; i ++){\n" +
					"\t\t\t\t\t\ttmp += \"0\";\n" +
					"\t\t\t\t\t}\n" +
					"\t\t\t\t\t_fileSeq = tmp+_fileSeq;\n" +
					"\t\t\t\t}\n" +
					"\n" +
					"\n" +
					"\n" +
					"\t\t\t\treturn \"hbase\" + \"_\"+time +\"_\" + _fileSeq+\".txt\";\n" +
					"\t\t\t}\n" +
					"\t\t});\n" +
					"\t\t//指定文件中每条记录格式，不指定默认为json格式输出\n" +
					"\t\tfileFtpOupputConfig.setRecordGenerator(new RecordGenerator() {\n" +
					"\t\t\t@Override\n" +
					"\t\t\tpublic void buildRecord(Context taskContext, CommonRecord record, Writer builder) {\n" +
					"\t\t\t\t//直接将记录按照json格式输出到文本文件中\n" +
					"\t\t\t\tSerialUtil.normalObject2json(record.getDatas(),//获取记录中的字段数据\n" +
					"\t\t\t\t\t\tbuilder);\n" +
					"\t\t\t\tString data = (String)taskContext.getTaskContext().getTaskData(\"data\");//从任务上下文中获取本次任务执行前设置时间戳\n" +
					"//          System.out.println(data);\n" +
					"\n" +
					"\t\t\t}\n" +
					"\t\t});\n" +
					"\t\timportBuilder.setFileFtpOupputConfig(fileFtpOupputConfig);");
	}







}
