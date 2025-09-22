package org.frameworkset.tran.input.csv;

import com.frameworkset.util.SimpleStringUtil;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.input.file.CustomFileReader;
import org.frameworkset.tran.input.file.FileListenerService;
import org.frameworkset.tran.input.file.FileLogRecord;
import org.frameworkset.tran.input.file.FileReaderTask;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.record.CellMapping;
import org.frameworkset.tran.record.CommonData;
import org.frameworkset.tran.record.FieldMappingManager;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.DataFormatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * @author xutengfei, yin-bp@163.com
 * @description
 * @create 2021/3/15
 */
public class CSVFileReaderTask extends FileReaderTask {
	private static Logger logger = LoggerFactory.getLogger(CSVFileReaderTask.class);
	private CSVFileConfig csvFileConfig;
	private CSVReader reader;
    private static CSVParser CSV_PARSER = new CSVParserBuilder()
            .withSeparator(',')
            .withQuoteChar('"')
            .build();

	public CSVFileReaderTask(TaskContext taskContext, File file, String fileId, CSVFileConfig fileConfig,
                             FileListenerService fileListenerService,
                             BaseDataTran fileDataTran,
                             Status currentStatus, FileInputConfig fileImportConfig) {

		super(taskContext, file, fileId, fileConfig,
				fileListenerService,
				fileDataTran,
				currentStatus, fileImportConfig);
        csvFileConfig = fileConfig;

	}

	public CSVFileReaderTask(String fileId, Status currentStatus, FileInputConfig fileImportConfig) {
		super(fileId, currentStatus, fileImportConfig);
	}


	public CSVFileReaderTask(TaskContext taskContext, File file, String fileId, CSVFileConfig fileConfig, long pointer, FileListenerService fileListenerService, BaseDataTran fileDataTran,
                             Status currentStatus, FileInputConfig fileImportConfig) {
		this(taskContext, file, fileId, fileConfig, fileListenerService, fileDataTran, currentStatus, fileImportConfig);

		this.pointer = pointer;
	}

	public void start() {
		String threadName = null;
		if (fileConfig.isEnableInode()) {
			threadName = "CSVFileReaderTask-Thread|" + fileInfo.getFilePath() + "|" + fileInfo.getFileId();
		} else {
			threadName = "CSVFileReaderTask-Thread|" + fileInfo.getFilePath();

		}
		registEndJob();
//        worker.setDaemon(true);
		worker = new Thread(new Work(), threadName);
		if (logger.isInfoEnabled())
			logger.info(threadName + " started.Current Status is "+currentStatus.toString());
		worker.start();

	}

	@Override
	protected void execute() {
		boolean reachEOFClosed = false;
		File file = fileInfo.getFile();
		DataTranPlugin dataTranPlugin = fileListenerService.getBaseDataTranPlugin();
		InputPlugin inputPlugin = dataTranPlugin.getInputPlugin();
		if (taskEnded || inputPlugin.isStopCollectData())
			return;

		try {
            DataFormatUtil.initDateformatThreadLocal();
			//获取sheet
			//获取sheet
//            synchronized (this){  单线程处理，无需同步处理

			if (reader == null) {
                if(csvFileConfig.getCharset() != null) {
                    reader = new CSVReaderBuilder(new CustomFileReader(file,csvFileConfig.getCharset()))
                            .withCSVParser(CSV_PARSER) // 复用已定义的 CSV_PARSER
                            .build();
                }
                else{
                    reader = new CSVReaderBuilder(new CustomFileReader(file))
                            .withCSVParser(CSV_PARSER) // 复用已定义的 CSV_PARSER
                            .build();
                }
                 
             

			}

//			if (pointer > reader.getRecordsRead()) {
//				pointer = 0l;
//			}
			int startRow = (int)pointer;
			if (pointer == 0l) {
				if (fileConfig.getSkipHeaderLines() > 0) {
					startRow = fileConfig.getSkipHeaderLines();
					pointer = fileConfig.getSkipHeaderLines();
				}
			}
			if (pointer < startRow) {
				pointer = startRow;
			}
			List<Record> recordList = new ArrayList<Record>();
			//批量处理记录数
			int fetchSize = this.fileListenerService.getImportContext().getFetchSize();

//			int rows = sheet.getPhysicalNumberOfRows();
//			reachEOFClosed = pointer == rows;
			//循环取每行的数据, 第一行为标题，从第二行开始

            boolean sendEOFRecord = false;
            if(pointer > 0){
                reader.skip((int)pointer);
            }
            
            String[] messageArr = null;
            while (true) {
                messageArr = reader.readNext();
                if(messageArr == null){
                    reachEOFClosed = true;                   
                    break;
                }
                resultOfCSV(file, pointer, messageArr, recordList, reachEOFClosed);
                if(inputPlugin.isStopCollectData()){
                    break;
                }
                //分批处理数据
                if (fetchSize > 0 && (recordList.size() >= fetchSize)) {
                    fileDataTran.appendData(new CommonData(recordList));
                    try {
                        fetchAwaitSleep();
                    } catch (InterruptedException e) {
                        break;
                    }
                    recordList = new ArrayList<Record>();
                }
            }
			
			if (recordList.size() > 0) {
				fileDataTran.appendData(new CommonData(recordList));
				try {
                    if(!inputPlugin.isStopCollectData())
					    fetchAwaitSleep();
				} catch (InterruptedException e) {

				}
			}
			//如果设置了文件结束，及结束作业，则进行相应处理，需迁移到通道结束处进行归档和删除处理
			if (reachEOFClosed) {
                if(!sendEOFRecord) {
                    /**
                     * 发送空记录
                     */

                    sendReadEOFcloseEvent(pointer);
                }
				taskEnded();


			}
		} catch (InterruptedException e){
			logger.error("",e);
//            throw new DataImportException("",e);
		} catch (Exception e) {
//            logger.error("",e);
			throw new DataImportException("", e);
		} finally {
            DataFormatUtil.releaseDateformatThreadLocal();
			destroy();

		}

	}
   
	public void destroy() {
        if(reader != null){
            try {
                reader.close();
                reader = null;
            } catch (IOException e) {
//                throw new RuntimeException(e);
            }
        }
		 
	}

	private void resultOfCSV(File file, long pointer, String[] csvRow, List<Record> recordList, boolean reachEOFClosed) {
		List<CellMapping> cellMappings = csvFileConfig.getCellMappingList();
		if (cellMappings == null || cellMappings.size() == 0) {
			throw new DataImportException("未指定cell与字段映射关系，参考文档：https://esdoc.bbossgroups.com/#/filelog-guide");
		}
        int cellSize = csvFileConfig.getMaxCellIndex() + 1;
        int realMaxCellIndex =  csvRow.length - 1;
        if(csvRow.length < cellSize){
            if(csvFileConfig.getMaxCellIndexMatchesFailedPolicy() == FieldMappingManager.MAX_CELL_INDEX_MATCHES_FAILED_POLICY_WARN_USENULLVALUE) {
                if (logger.isWarnEnabled()) {
                    logger.warn("csv文件列数小于配置的最大列数：" + cellSize + "，csvRow.length：" + csvRow.length + ",请检查csv文件列数是否正确，参考文档：https://esdoc.bbossgroups.com/#/filelog-guide,data:" + SimpleStringUtil.object2json(csvRow));
                }
            }
            else if(csvFileConfig.getMaxCellIndexMatchesFailedPolicy() == FieldMappingManager.MAX_CELL_INDEX_MATCHES_FAILED_POLICY_THROW_EXCEPTION) {

                throw new DataImportException("csv文件列数小于配置的最大列数："+cellSize+"，csvRow.length："+csvRow.length+",请检查csv文件列数是否正确，参考文档：https://esdoc.bbossgroups.com/#/filelog-guide,data:"+SimpleStringUtil.object2json(csvRow));
            }
            else if(csvFileConfig.getMaxCellIndexMatchesFailedPolicy() == FieldMappingManager.MAX_CELL_INDEX_MATCHES_FAILED_POLICY_IGNORE_RECORD) {
                if(!reachEOFClosed){
                    recordList.add(new FileLogRecord(taskContext,this.fileDataTran.getImportContext(),true,pointer,reachEOFClosed,false));
                }
                return;
            }
        }
		boolean allIsNull = true;
		Map json = new LinkedHashMap();
            
		String xssfCell = null;
		for (CellMapping cellMapping : cellMappings) {
			try {
                if(cellMapping.getCell() > realMaxCellIndex){
                    if (cellMapping.getDefaultValue() != null) {
                        json.put(cellMapping.getFieldName(), cellMapping.getDefaultValue());
                    } else {
                        json.put(cellMapping.getFieldName(), null);
                    }
                    continue;
                }
				xssfCell = csvRow[cellMapping.getCell()];

				if (xssfCell == null) {
					if (cellMapping.getDefaultValue() != null) {
						json.put(cellMapping.getFieldName(), cellMapping.getDefaultValue());
					} else {
						json.put(cellMapping.getFieldName(), null);
					}
					continue;
				}
				if (cellMapping.getCellType() == CellMapping.CELL_STRING) {
					
					json.put(cellMapping.getFieldName(), xssfCell);

					allIsNull = false;
					/**
					 * CELL_NUMBER_INTEGER = 6;
					 * 	public static final int CELL_NUMBER_LONG = 7;
					 * 	public static final int CELL_NUMBER_FLOAT = 8;
					 * 	public static final int CELL_NUMBER_SHORT = 9;
					 */
				} else if (cellMapping.getCellType() == CellMapping.CELL_NUMBER) {
					double value = 0d;
					 
					value = Double.valueOf(xssfCell);


					json.put(cellMapping.getFieldName(), value);
					allIsNull = false;
				} else if (cellMapping.getCellType() == CellMapping.CELL_NUMBER_INTEGER) {
					
					json.put(cellMapping.getFieldName(), Integer.parseInt(xssfCell));
					allIsNull = false;
				} else if (cellMapping.getCellType() == CellMapping.CELL_NUMBER_LONG) {
                    json.put(cellMapping.getFieldName(), Long.parseLong(xssfCell));
					allIsNull = false;
				} else if (cellMapping.getCellType() == CellMapping.CELL_NUMBER_FLOAT) {
                    json.put(cellMapping.getFieldName(), Float.parseFloat(xssfCell));
					allIsNull = false;
				} else if (cellMapping.getCellType() == CellMapping.CELL_NUMBER_SHORT) {
                    json.put(cellMapping.getFieldName(), Short.parseShort(xssfCell));
					allIsNull = false;
				} else if (cellMapping.getCellType() == CellMapping.CELL_DATE) {
                    String dateFormatter = cellMapping.getDateFormat();
                    if(SimpleStringUtil.isEmpty(dateFormatter)){
                        dateFormatter = "yyyy-MM-dd HH:mm:ss";
                    }
					Date value = DataFormatUtil.getDate(dateFormatter,xssfCell);
					json.put(cellMapping.getFieldName(), value);
					allIsNull = false;
				} else if (cellMapping.getCellType() == CellMapping.CELL_BOOLEAN) {

                    json.put(cellMapping.getFieldName(), Boolean.parseBoolean(xssfCell));
					allIsNull = false;
				} else {
                    json.put(cellMapping.getFieldName(), xssfCell);
					allIsNull = false;
				}
			}
			catch (Exception e){
				if(xssfCell != null) {
					throw new DataImportException("cell mapping:" + cellMapping.toString() + ",cell value:" + xssfCell, e);
				}
				else{
					throw new DataImportException("cell mapping:" + cellMapping.toString() + ",cell values:" + SimpleStringUtil.object2json(csvRow) , e);
				}
			}

		}
		if(!allIsNull) {
			Map addFields = this.getAddFields();
			if (addFields != null && addFields.size() > 0) {
				json.putAll(addFields);
			}
			Map common = common(file, pointer);
			if (enableMeta) {
				json.put("@filemeta", common);
				json.put("@timestamp", new Date());
			}
			recordList.add(new FileLogRecord(taskContext,this.fileDataTran.getImportContext(), common, json, pointer, reachEOFClosed));
		}
		else if(!reachEOFClosed){
			recordList.add(new FileLogRecord(taskContext,this.fileDataTran.getImportContext(),true,pointer,reachEOFClosed,false));
		}


	}





}
