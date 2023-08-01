package org.frameworkset.tran.input.word;

import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.input.file.FileListenerService;
import org.frameworkset.tran.input.file.FileLogRecord;
import org.frameworkset.tran.input.file.FileReaderTask;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.record.CommonData;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * @author  yin-bp@163.com
 * @description
 * @create 2021/3/15
 */
public class WordFileReaderTask extends FileReaderTask {
	private static Logger logger = LoggerFactory.getLogger(WordFileReaderTask.class);
	private WordFileConfig wordFileConfig;


	public WordFileReaderTask(TaskContext taskContext, File file, String fileId, WordFileConfig wordFileConfig,
                              FileListenerService fileListenerService,
                              BaseDataTran fileDataTran,
                              Status currentStatus, FileInputConfig fileImportConfig) {

		super(taskContext, file, fileId, wordFileConfig,
				fileListenerService,
				fileDataTran,
				currentStatus, fileImportConfig);
        this.wordFileConfig = wordFileConfig;

	}

	public WordFileReaderTask(String fileId, Status currentStatus, FileInputConfig fileImportConfig) {
		super(fileId, currentStatus, fileImportConfig);
	}




	public void start() {
		String threadName = null;
		if (fileConfig.isEnableInode()) {
			threadName = "WordFileReaderTask-Thread|" + fileInfo.getFilePath() + "|" + fileInfo.getFileId();
		} else {
			threadName = "WordFileReaderTask-Thread|" + fileInfo.getFilePath();

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
     * @return XWPFDocument
     */

	@Override
	protected void execute() {
		boolean reachEOFClosed = false;
		File file = fileInfo.getFile();
		DataTranPlugin dataTranPlugin = fileListenerService.getBaseDataTranPlugin();
		InputPlugin inputPlugin = dataTranPlugin.getInputPlugin();
		if (taskEnded || inputPlugin.isStopCollectData())
			return;
        FileInputStream fis = null;
        XWPFDocument document = null;

        try {
            pointer = file.length();
            List<Record> recordList = new ArrayList<Record>();
            reachEOFClosed = true;
            if(pointer > 0) {
                fis = new FileInputStream(file);
                document = new XWPFDocument(fis);
                XWPFWordExtractor extractor = new XWPFWordExtractor(document);



                result(file, pointer, extractor, recordList, reachEOFClosed);
            }
            else{
                result(file, pointer, null, recordList, reachEOFClosed);
            }
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
            if(document != null){
                try {
                    document.close();
                } catch (IOException e) {

                }
            }
            if(fis != null){
                try {
                    fis.close();
                } catch (IOException e) {

                }
            }
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

	private void result(File file, long pointer, XWPFWordExtractor extractor, List<Record> recordList, boolean reachEOFClosed) throws Exception {

		Map json = new LinkedHashMap();

        if(wordFileConfig.getWordExtractor() != null){
            wordFileConfig.getWordExtractor().extractor(json,file,extractor);
        }
        else{
            if(extractor != null) {
                json.put("wordContent", extractor.getText());
            }
            else{
                json.put("wordContent", "");
            }
        }


        Map addFields = this.getAddFields();
        if (addFields != null && addFields.size() > 0) {
            json.putAll(addFields);
        }
        Map common = common(file, pointer, json);
        if (enableMeta) {
            json.put("@filemeta", common);
            json.put("@timestamp", new Date());
        }
        recordList.add(new FileLogRecord(taskContext, common, json, pointer, reachEOFClosed));



	}





}
