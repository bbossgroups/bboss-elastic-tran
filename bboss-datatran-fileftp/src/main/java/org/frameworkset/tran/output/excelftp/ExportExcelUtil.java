/**
 * Copyright &copy; 2012-2014 <a href="https://github.com/thinkgem/jeesite">JeeSite</a> All rights reserved.
 */
package org.frameworkset.tran.output.excelftp;

import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.plugin.file.output.ExcelFileOutputConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 导出Excel文件（导出“XLSX”格式，支持大数据量导出   @see org.apache.poi.ss.SpreadsheetVersion）
 *
 * @author biaoping.yin
 * @version 2020-10-30
 */
public class ExportExcelUtil extends BaseExcelInf{

    private static Logger log = LoggerFactory.getLogger(ExportExcelUtil.class);
    private Map<String,ExportExcel> exportExcelMap = new HashMap<>();
    private ExportExcel currentExportExcel;
    public SXSSFWorkbook getWb() {
        return wb;
    }


    /**
     * map数据生成sheet
//     * @param sheetName sheet名称
//     * @param dataTitle 标题
//     * @param headerList 表头
//     * @param list
//     * @param columns map数据key值
     * @return
     */
    public ExportExcelUtil buildSheet(ExcelFileOutputConfig excelFileOutputConfig){
        currentExportExcel = new ExportExcel(excelFileOutputConfig,wb, excelFileOutputConfig.getSheetName(), excelFileOutputConfig.getTitle(), excelFileOutputConfig.getCellMappingList());
        exportExcelMap.put(excelFileOutputConfig.getSheetName(),currentExportExcel);
//        new ExportExcel(wb,sheetName,dataTitle, headerList).setMapDataList(list, columns);
        return this;
    }


    /**
     * 输出数据流
     *
     * @param filePath 输出数据流
     */
    public void write(String filePath) throws IOException {
        FileOutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(filePath);
            wb.write(outputStream);
        }
        finally {
            if(outputStream != null ){
                outputStream.close();
            }
        }

    }

    /**
     * 构造函数

     */
    public ExportExcelUtil() {

    }
    public void writeData(List<CommonRecord> datas) throws IOException {
        currentExportExcel.writeData(datas);
    }
    /**

     */
    public void initialize(int rowAccessWindowSize) {
        this.wb = new SXSSFWorkbook(rowAccessWindowSize);

        if(log.isDebugEnabled())
            log.debug("SXSSFWorkbook Initialize success.");
    }





    /**
     * 清理临时文件
     */
    public ExportExcelUtil dispose() {
        wb.dispose();
        return this;
    }


}
