/**
 * Copyright &copy; 2012-2014 <a href="https://github.com/thinkgem/jeesite">JeeSite</a> All rights reserved.
 */
package org.frameworkset.tran.output.excelftp;


import com.frameworkset.util.SimpleStringUtil;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFClientAnchor;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.input.excel.CellMapping;
import org.frameworkset.tran.plugin.file.output.ExcelFileOutputConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * 导出Excel文件（导出“XLSX”格式，支持大数据量导出   @see org.apache.poi.ss.SpreadsheetVersion）
 *
 * @author biaoping.yin
 * @version 2020-06-21
 */
public class ExportExcel extends BaseExcelInf{

    private static final Logger log = LoggerFactory.getLogger(ExportExcel.class);
    private ExcelFileOutputConfig excelFileOutputConfig;


    /**
     * 工作表对象
     */
    private SXSSFSheet sheet;

    /**
     * 样式列表
     */
    private Map<String, CellStyle> styles;

    /**
     * 当前行号
     */
    private int rownum;
    private List<CellMapping> cellMappingList;

    /**
     * 注解列表（Object[]{ ExcelField, Field/Method }）
     */
    List<Object[]> annotationList = new ArrayList<>();





    /**
     * 构造函数
     *
     * @param title      表格标题，传“空值”，表示无标题
     * @param cellMappingList 表头列表
     */
    public ExportExcel(ExcelFileOutputConfig excelFileOutputConfig, SXSSFWorkbook sxssfWorkbook, String sheetName, String title, List<CellMapping> cellMappingList) {
        this.cellMappingList = cellMappingList;
        this.excelFileOutputConfig = excelFileOutputConfig;
        initialize(sxssfWorkbook,  sheetName,title, cellMappingList);
    }
    private String handleSheetName(String sheetName){
        if(sheetName == null || sheetName.equals("")){
            sheetName = "sheet1";
            return sheetName;
        }
        sheetName = sheetName.replace("*","");
        sheetName = sheetName.replace("/","");
        sheetName = sheetName.replace(":","");
        sheetName = sheetName.replace("?","");
        sheetName = sheetName.replace("[","");
        sheetName = sheetName.replace("\\","");
        sheetName = sheetName.replace("]","");
        return sheetName;
    }
    /**
     * 初始化函数
     *
     * @param title      表格标题，传“空值”，表示无标题
     * @param cellMappingList 表头列表
     */
    private void initialize(SXSSFWorkbook sxssfWorkbook,String sheetName,String title, List<CellMapping> cellMappingList) {
        this.wb = sxssfWorkbook == null?new SXSSFWorkbook(excelFileOutputConfig.getFlushRows()):sxssfWorkbook;
        this.sheet = wb.createSheet(handleSheetName( sheetName));
        this.sheet.trackAllColumnsForAutoSizing();
        this.styles = createStyles(wb);
        // Create title
        if (SimpleStringUtil.isNotEmpty(title)) {
            Row titleRow = sheet.createRow(rownum++);
            titleRow.setHeightInPoints(30);
            Cell titleCell = titleRow.createCell(0);
            titleCell.setCellStyle(styles.get("title"));
            titleCell.setCellValue(new XSSFRichTextString(title));
            if(cellMappingList.size() > 1) {
                sheet.addMergedRegion(new CellRangeAddress(titleRow.getRowNum(),
                        titleRow.getRowNum(), titleRow.getRowNum(), cellMappingList.size() - 1));
            }
        }
        // Create header
        if (cellMappingList == null) {
            throw new RuntimeException("headerList not null!");
        }
        Row headerRow = sheet.createRow(rownum++);
        headerRow.setHeightInPoints(16);
        for (int i = 0; i < cellMappingList.size(); i++) {
            CellMapping cellMapping = cellMappingList.get(i);
            Cell cell = headerRow.createCell(cellMapping.getCell());
            cell.setCellStyle(styles.get("header"));

            String cellTitle = cellMapping.getCellTitle();
            if(cellTitle == null)
                cellTitle = cellMapping.getFieldName();

            if (cellMapping.getCellComment() != null) {
                Comment comment = this.sheet.createDrawingPatriarch().createCellComment(
                        new XSSFClientAnchor(0, 0, 0, 0, (short) 3, 3, (short) 5, 6));
                comment.setString(new XSSFRichTextString(cellMapping.getCellComment()));
                cell.setCellComment(comment);
            }
                cell.setCellValue(new XSSFRichTextString(cellTitle));
            sheet.autoSizeColumn(cellMapping.getCell());
        }
        for (int i = 0; i < cellMappingList.size(); i++) {
            CellMapping cellMapping = cellMappingList.get(i);
            int colWidth = sheet.getColumnWidth(cellMapping.getCell()) * 2;
            sheet.setColumnWidth(cellMapping.getCell(), colWidth < 5000 ? 5000 : colWidth);
        }
        sheet.untrackAllColumnsForAutoSizing();
        if(log.isDebugEnabled())
            log.debug("Initialize success.");
    }

    /**
     * 创建表格样式
     *
     * @param wb 工作薄对象
     * @return 样式列表
     */
    private Map<String, CellStyle> createStyles(Workbook wb) {
        Map<String, CellStyle> styles = new HashMap<String, CellStyle>();

        CellStyle style = wb.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        Font titleFont = wb.createFont();
        titleFont.setFontName("Arial");
        titleFont.setFontHeightInPoints((short) 16);
        titleFont.setBold(true);
        style.setFont(titleFont);
        styles.put("title", style);

        style = wb.createCellStyle();
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        style.setBorderRight(BorderStyle.THIN);
        style.setRightBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
        style.setBorderLeft(BorderStyle.THIN);
        style.setLeftBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
        style.setBorderTop(BorderStyle.THIN);
        style.setTopBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
        style.setBorderBottom(BorderStyle.THIN);
        style.setBottomBorderColor(IndexedColors.GREY_50_PERCENT.getIndex());
        Font dataFont = wb.createFont();
        dataFont.setFontName("Arial");
        dataFont.setFontHeightInPoints((short) 10);
        style.setFont(dataFont);
        styles.put("data", style);

        style = wb.createCellStyle();
        style.cloneStyleFrom(styles.get("data"));
        style.setAlignment(HorizontalAlignment.LEFT);
        styles.put("data1", style);

        style = wb.createCellStyle();
        style.cloneStyleFrom(styles.get("data"));
        style.setAlignment(HorizontalAlignment.CENTER);
        styles.put("data2", style);

        style = wb.createCellStyle();
        style.cloneStyleFrom(styles.get("data"));
        style.setAlignment(HorizontalAlignment.RIGHT);
        styles.put("data3", style);

        style = wb.createCellStyle();
        style.cloneStyleFrom(styles.get("data"));
//		style.setWrapText(true);
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setFillForegroundColor(IndexedColors.GREY_50_PERCENT.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        Font headerFont = wb.createFont();
        headerFont.setFontName("Arial");
        headerFont.setFontHeightInPoints((short) 10);
        headerFont.setBold(true);
        headerFont.setColor(IndexedColors.WHITE.getIndex());
        style.setFont(headerFont);
        styles.put("header", style);

        return styles;
    }

    /**
     * 添加一行
     *
     * @return 行对象
     */
    public Row addRow() {
        return sheet.createRow(rownum++);
    }




    /**
     * 添加一个单元格
     *
     * @param row    添加的行
     * @param column 添加列号
     * @param val    添加值
     * @param align  对齐方式（1：靠左；2：居中；3：靠右）
     * @return 单元格对象
     */
    public Cell addCell(CellMapping cellMapping,Row row, int column, Object val, int align, Class<?> fieldType) {
        Cell cell = row.createCell(column);
        CellStyle style = styles.get("data" + (align >= 1 && align <= 3 ? align : ""));
        try {
            if (val instanceof String) {
                cell.setCellValue(new XSSFRichTextString((String) val));
            } else if (val instanceof Integer) {

                if(cellMapping.getNumberFormat() != null && !cellMapping.getNumberFormat().equals("")) {
                    DataFormat format = wb.createDataFormat();
                    style.setDataFormat(format.getFormat(cellMapping.getNumberFormat()));
                }

                cell.setCellValue((Integer) val);
            } else if (val instanceof Long) {
                if(cellMapping.getNumberFormat() != null && !cellMapping.getNumberFormat().equals("")) {
                    DataFormat format = wb.createDataFormat();
                    style.setDataFormat(format.getFormat(cellMapping.getNumberFormat()));
                }
                cell.setCellValue((Long) val);
            } else if (val instanceof Double) {
                if(cellMapping.getNumberFormat() != null && !cellMapping.getNumberFormat().equals("")) {
                    DataFormat format = wb.createDataFormat();
                    style.setDataFormat(format.getFormat(cellMapping.getNumberFormat()));
                }
                cell.setCellValue((Double) val);
            } else if (val instanceof Float) {
                if(cellMapping.getNumberFormat() != null && !cellMapping.getNumberFormat().equals("")) {
                    DataFormat format = wb.createDataFormat();
                    style.setDataFormat(format.getFormat(cellMapping.getNumberFormat()));
                }
                cell.setCellValue((Float) val);
            } else if (val instanceof Date) {
                DataFormat format = wb.createDataFormat();
                if(cellMapping.getDateFormat() == null || cellMapping.getDateFormat().equals("")) {
                    style.setDataFormat(format.getFormat("yyyy-MM-dd HH:mm:ss"));
                }
                else{
                    style.setDataFormat(format.getFormat(cellMapping.getDateFormat()));
                }
                cell.setCellValue((Date) val);

            } else {
                if (fieldType != Class.class) {
                    cell.setCellValue( new XSSFRichTextString(SimpleStringUtil.object2json(val)));
                }
            }
        } catch (Exception ex) {
            if(log.isErrorEnabled())
                log.error("Set cell value [" + row.getRowNum() + "," + column + "] error: " + ex.toString(),ex);
            cell.setCellValue(new XSSFRichTextString(val.toString()));
        }
        cell.setCellStyle(style);
        return cell;
    }

    public void writeData(List<CommonRecord> datas) throws IOException {
        if(datas == null || datas.size() == 0)
            return ;
        for (CommonRecord e : datas) {
            int column = 0;
            Row row = this.addRow();
            StringBuilder sb = new StringBuilder();

            for (CellMapping cellMapping : cellMappingList) {

                Object val = e.getData(cellMapping.getFieldName());


                if(val == null){
                    val = cellMapping.getDefaultValue();
                }
                if(val == null){
                    val = "";
                }
                this.addCell(cellMapping,row, cellMapping.getCell(), val, 1, val.getClass());
                if(log.isDebugEnabled())
                    sb.append(val + ", ");
            }
            if(log.isDebugEnabled())
                log.debug("Write success: [" + row.getRowNum() + "] " + sb.toString());
        }
    }









    /**
     * 清理临时文件
     */
    public ExportExcel dispose() {
        wb.dispose();
        return this;
    }

    public SXSSFWorkbook getWb() {
        return wb;
    }
//	/**
//	 * 导出测试
//	 */
//	public static void main(String[] args) throws Throwable {
//		
//		List<String> headerList = Lists.newArrayList();
//		for (int i = 1; i <= 10; i++) {
//			headerList.add("表头"+i);
//		}
//		
//		List<String> dataRowList = Lists.newArrayList();
//		for (int i = 1; i <= headerList.size(); i++) {
//			dataRowList.add("数据"+i);
//		}
//		
//		List<List<String>> dataList = Lists.newArrayList();
//		for (int i = 1; i <=1000000; i++) {
//			dataList.add(dataRowList);
//		}
//
//		ExportExcel ee = new ExportExcel("表格标题", headerList);
//		
//		for (int i = 0; i < dataList.size(); i++) {
//			Row row = ee.addRow();
//			for (int j = 0; j < dataList.get(i).size(); j++) {
//				ee.addCell(row, j, dataList.get(i).get(j));
//			}
//		}
//		
//		ee.writeFile("target/export.xlsx");
//
//		ee.dispose();
//		
//		log.debug("Export success.");
//		
//	}

}
