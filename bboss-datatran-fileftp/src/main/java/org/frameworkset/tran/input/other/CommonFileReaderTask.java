package org.frameworkset.tran.input.other;

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.input.file.FileListenerService;
import org.frameworkset.tran.input.file.FileLogRecord;
import org.frameworkset.tran.input.file.FileReaderTask;
import org.frameworkset.tran.input.file.RecordExtractor;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.record.CommonData;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author  yin-bp@163.com
 * @description
 * @create 2021/3/15
 */
public class CommonFileReaderTask extends FileReaderTask {
	private static Logger logger = LoggerFactory.getLogger(CommonFileReaderTask.class);
	private CommonFileConfig commonFileConfig;


	public CommonFileReaderTask(TaskContext taskContext, File file, String fileId, CommonFileConfig commonFileConfig,
                                FileListenerService fileListenerService,
                                BaseDataTran fileDataTran,
                                Status currentStatus, FileInputConfig fileImportConfig) {

		super(taskContext, file, fileId, commonFileConfig,
				fileListenerService,
				fileDataTran,
				currentStatus, fileImportConfig);
        this.commonFileConfig = commonFileConfig;

	}

	public CommonFileReaderTask(String fileId, Status currentStatus, FileInputConfig fileImportConfig) {
		super(fileId, currentStatus, fileImportConfig);
	}




	public void start() {
		String threadName = null;
		if (fileConfig.isEnableInode()) {
			threadName = "CommonFileReaderTask-Thread|" + fileInfo.getFilePath() + "|" + fileInfo.getFileId();
		} else {
			threadName = "CommonFileReaderTask-Thread|" + fileInfo.getFilePath();

		}
		registEndJob();
//        worker.setDaemon(true);
		worker = new Thread(new Work(), threadName);
		if (logger.isInfoEnabled())
			logger.info(threadName + " started.Current Status is "+currentStatus.toString());
		worker.start();

	}
    /**
     * 读取文件
     */

	@Override
	protected void execute() {
		boolean reachEOFClosed = false;
		File file = fileInfo.getFile();
		DataTranPlugin dataTranPlugin = fileListenerService.getBaseDataTranPlugin();
		InputPlugin inputPlugin = dataTranPlugin.getInputPlugin();
		if (taskEnded || inputPlugin.isStopCollectData())
			return;

        try {
            pointer = file.length();
            List<Record> recordList = new ArrayList<Record>();
            reachEOFClosed = true;




            resultOfCommon(file, pointer,   recordList, reachEOFClosed);

            if(recordList.size() > 0 )
                fileDataTran.appendData(new CommonData(recordList));
			//如果设置了文件结束，及结束作业，则进行相应处理，需迁移到通道结束处进行归档和删除处理
            /**
             * 发送空记录
             */

            sendReadEOFcloseEvent(pointer);
            taskEnded();


		} catch (InterruptedException e){
			logger.error("",e);
//            throw new DataImportException("",e);
		} catch (Exception e) {
//            logger.error("",e);
			throw new DataImportException("", e);
		} finally {

//			try {
//				//需要删除采集完数据的eof文件，有必要进行优化并在回调函数中处理
//				if (reachEOFClosed) {
//					if (fileImportConfig.isBackupSuccessFiles())//备份采集完的数据文件，默认保留一周，过期清理
//						backupFile(currentStatus.getRelativeParentDir(), file);
//					else if (fileConfig.isDeleteEOFFile())//删除日志文件
//						file.delete();
//
//				}
//			} catch (Exception e) {
//				logger.warn("", e);
//			}
		}

	}

	public void destroy() {

	}

	private void resultOfCommon(File file, long pointer,List<Record> recordList, boolean reachEOFClosed) throws Exception {

        if(commonFileConfig.getCommonFileExtractor() != null){
            RecordExtractor<File> recordExtractor = new RecordExtractor<>(file, fileConfig.getImportContext(),file);
            commonFileConfig.getCommonFileExtractor().extractor(recordExtractor);
            List<Map> records = recordExtractor.getRecords();
            if(records != null && records.size() > 0) {
                Map addFields = this.getAddFields();
                Map common = common(file, pointer);
                for (int i = 0; i < records.size(); i++) {
                    Map json = records.get(i);
                    if (addFields != null && addFields.size() > 0) {
                        json.putAll(addFields);
                    }
                    if (enableMeta) {
                        json.put("@filemeta", common);
                        json.put("@timestamp", new Date());
                    }
                    recordList.add(new FileLogRecord(taskContext,this.fileDataTran.getImportContext(), common, json, pointer, reachEOFClosed));
                }
            }

        }


	}





}
