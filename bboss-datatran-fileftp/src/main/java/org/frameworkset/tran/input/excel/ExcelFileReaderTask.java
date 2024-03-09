package org.frameworkset.tran.input.excel;

import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.input.file.FileListenerService;
import org.frameworkset.tran.input.file.FileLogRecord;
import org.frameworkset.tran.input.file.FileReaderTask;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.record.CellMapping;
import org.frameworkset.tran.record.CommonData;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author xutengfei, yin-bp@163.com
 * @description
 * @create 2021/3/15
 */
public class ExcelFileReaderTask extends FileReaderTask {
	private static Logger logger = LoggerFactory.getLogger(ExcelFileReaderTask.class);
	private ExcelFileConfig excelFileConfig;
	private XSSFWorkbook sheets;
	private XSSFSheet sheet;

	public ExcelFileReaderTask(TaskContext taskContext, File file, String fileId, ExcelFileConfig fileConfig,
							   FileListenerService fileListenerService,
							   BaseDataTran fileDataTran,
							   Status currentStatus, FileInputConfig fileImportConfig) {

		super(taskContext, file, fileId, fileConfig,
				fileListenerService,
				fileDataTran,
				currentStatus, fileImportConfig);
		excelFileConfig = fileConfig;

	}

	public ExcelFileReaderTask(String fileId, Status currentStatus, FileInputConfig fileImportConfig) {
		super(fileId, currentStatus, fileImportConfig);
	}


	public ExcelFileReaderTask(TaskContext taskContext, File file, String fileId, ExcelFileConfig fileConfig, long pointer, FileListenerService fileListenerService, BaseDataTran fileDataTran,
							   Status currentStatus, FileInputConfig fileImportConfig) {
		this(taskContext, file, fileId, fileConfig, fileListenerService, fileDataTran, currentStatus, fileImportConfig);

		this.pointer = pointer;
	}

	public void start() {
		String threadName = null;
		if (fileConfig.isEnableInode()) {
			threadName = "ExcelFileReaderTask-Thread|" + fileInfo.getFilePath() + "|" + fileInfo.getFileId();
		} else {
			threadName = "ExcelFileReaderTask-Thread|" + fileInfo.getFilePath();

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
			InputStream inputStream = new FileInputStream(file);
			//获取sheet
			//获取sheet
//            synchronized (this){  单线程处理，无需同步处理

			if (sheets == null) {
				sheets = new XSSFWorkbook(inputStream);
				sheet = sheets.getSheetAt(excelFileConfig.getSheet());

			}

			if (pointer > sheet.getPhysicalNumberOfRows()) {
				pointer = 0;
			}
			int startRow = new Long(pointer).intValue();
			if (pointer == 0) {
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

			int rows = sheet.getPhysicalNumberOfRows();
			reachEOFClosed = pointer == rows;
			//循环取每行的数据, 第一行为标题，从第二行开始

            boolean sendEOFRecord = false;
			for (int rowIndex = startRow; rowIndex < rows; rowIndex++) {
				XSSFRow xssfRow = sheet.getRow(rowIndex);
				if (xssfRow == null ) {

					reachEOFClosed = pointer == rows - 1;
					if(!reachEOFClosed){
						recordList.add(new FileLogRecord(taskContext,this.fileDataTran.getImportContext(),true,pointer,reachEOFClosed,false));
					}
					else if(inputPlugin.isStopCollectData()){
                        sendEOFRecord = true;
						recordList.add(new FileLogRecord(taskContext,this.fileDataTran.getImportContext(),true,pointer,reachEOFClosed,true));
						break;
					}
					pointer++;

					continue;
				}
				reachEOFClosed = pointer == rows - 1;
				result(file, pointer, xssfRow, recordList, reachEOFClosed);
				if(inputPlugin.isStopCollectData()){
					break;
				}
				pointer++;
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
			destroy();
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
		if (sheets != null) {
			try {
				sheets.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			sheets = null;
			sheet = null;
		}
	}

	private void result(File file, long pointer, XSSFRow xssfRow, List<Record> recordList, boolean reachEOFClosed) {
		List<CellMapping> cellMappings = excelFileConfig.getCellMappingList();
		if (cellMappings == null || cellMappings.size() == 0) {
			throw new DataImportException("未指定cell与字段映射关系，参考文档：https://esdoc.bbossgroups.com/#/filelog-guide");
		}
		boolean allIsNull = true;
		Map json = new LinkedHashMap();

		XSSFCell xssfCell = null;
		for (CellMapping cellMapping : cellMappings) {
			try {
				xssfCell = xssfRow.getCell(cellMapping.getCell());

				if (xssfCell == null) {
					if (cellMapping.getDefaultValue() != null) {
						json.put(cellMapping.getFieldName(), cellMapping.getDefaultValue());
					} else {
						json.put(cellMapping.getFieldName(), null);
					}
					continue;
				}
				CellType cellType = xssfCell.getCellType();
				if (cellMapping.getCellType() == CellMapping.CELL_STRING) {
					String value = null;
					if(cellType ==  CellType.NUMERIC){
						double _value = xssfCell.getNumericCellValue();
						value = String.valueOf(_value);
					}
					else {
						value = xssfCell.getStringCellValue();
					}
					Object _value = value;
					if (cellMapping.getDateFormat() != null) {
						SimpleDateFormat dateFormat = new SimpleDateFormat(cellMapping.getDateFormat());
						try {
							_value = dateFormat.parse(value);
						} catch (ParseException e) {
							logger.error("cell mapping:{},cell value:{}", cellMapping.toString(), value);
							logger.error("", e);
						}
					} else if (cellMapping.getNumberFormat() != null) {
						DecimalFormat numberFormat = new DecimalFormat(cellMapping.getNumberFormat());
						try {
							_value = numberFormat.parse(value);
						} catch (ParseException e) {
							logger.error("cell mapping:{},cell value:{}", cellMapping.toString(), value);
							logger.error("", e);
						}
					}
					json.put(cellMapping.getFieldName(), _value);

					allIsNull = false;
					/**
					 * CELL_NUMBER_INTEGER = 6;
					 * 	public static final int CELL_NUMBER_LONG = 7;
					 * 	public static final int CELL_NUMBER_FLOAT = 8;
					 * 	public static final int CELL_NUMBER_SHORT = 9;
					 */
				} else if (cellMapping.getCellType() == CellMapping.CELL_NUMBER) {
					double value = 0d;
					if(cellType ==  CellType.NUMERIC){
						value = xssfCell.getNumericCellValue();
					}
					else {
						String _value = xssfCell.getStringCellValue();
						value = Double.valueOf(_value);

					}

					json.put(cellMapping.getFieldName(), value);
					allIsNull = false;
				} else if (cellMapping.getCellType() == CellMapping.CELL_NUMBER_INTEGER) {
					Double value = null;
					if(cellType ==  CellType.NUMERIC){
						value = xssfCell.getNumericCellValue();
					}
					else {
						String _value = xssfCell.getStringCellValue();
						value = Double.valueOf(_value);

					}
					json.put(cellMapping.getFieldName(), value.intValue());
					allIsNull = false;
				} else if (cellMapping.getCellType() == CellMapping.CELL_NUMBER_LONG) {
					Double value = null;
					if(cellType ==  CellType.NUMERIC){
						value = xssfCell.getNumericCellValue();
					}
					else {
						String _value = xssfCell.getStringCellValue();
						value = Double.valueOf(_value);

					}
					json.put(cellMapping.getFieldName(), value.longValue());
					allIsNull = false;
				} else if (cellMapping.getCellType() == CellMapping.CELL_NUMBER_FLOAT) {
					Double value = null;
					if(cellType ==  CellType.NUMERIC){
						value = xssfCell.getNumericCellValue();
					}
					else {
						String _value = xssfCell.getStringCellValue();
						value = Double.valueOf(_value);

					}
					json.put(cellMapping.getFieldName(), value.floatValue());
					allIsNull = false;
				} else if (cellMapping.getCellType() == CellMapping.CELL_NUMBER_SHORT) {
					Double value = null;
					if(cellType ==  CellType.NUMERIC){
						value = xssfCell.getNumericCellValue();
					}
					else {
						String _value = xssfCell.getStringCellValue();
						value = Double.valueOf(_value);

					}
					json.put(cellMapping.getFieldName(), value.shortValue());
					allIsNull = false;
				} else if (cellMapping.getCellType() == CellMapping.CELL_DATE) {
					Date value = xssfCell.getDateCellValue();
					json.put(cellMapping.getFieldName(), value);
					allIsNull = false;
				} else if (cellMapping.getCellType() == CellMapping.CELL_BOOLEAN) {

					boolean value = false;
					if(cellType ==  CellType.BOOLEAN){
						value = xssfCell.getBooleanCellValue();
					}
					if(cellType ==  CellType.NUMERIC){
						double _value = xssfCell.getNumericCellValue();
						value = _value > 0;
					}
					else {
						String _value = xssfCell.getStringCellValue();
						value = Boolean.valueOf(_value);

					}

					json.put(cellMapping.getFieldName(), value);
					allIsNull = false;
				} else {
					String value = xssfCell.getStringCellValue();
					json.put(cellMapping.getFieldName(), value);
					allIsNull = false;
				}
			}
			catch (Exception e){
				if(xssfCell != null) {
					throw new DataImportException("cell mapping:" + cellMapping.toString() + ",cell value:" + xssfCell, e);
				}
				else{
					throw new DataImportException("cell mapping:" + cellMapping.toString() , e);
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
