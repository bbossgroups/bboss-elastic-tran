package org.frameworkset.tran.input.excel;

import org.frameworkset.tran.input.file.FileConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yinbp
 * @description
 * @create 2021/3/12
 */
public class ExcelFileConfig extends FileConfig {
    private Logger logger = LoggerFactory.getLogger(ExcelFileConfig.class);
    private List<CellMapping> cellMappingList;
    private Map<Integer,CellMapping> cellMappings;
    public ExcelFileConfig(){
        super();
        cellMappingList = new ArrayList<>();
        cellMappings = new LinkedHashMap<>();
    }
    public int getSheet() {
        return sheet;
    }

    public List<CellMapping> getCellMappingList() {
        return cellMappingList;
    }

    public ExcelFileConfig setSheet(int sheet) {
        this.sheet = sheet;
        return this;
    }
    public ExcelFileConfig addCellMapping(int cell,String field){
        CellMapping cellMapping = new CellMapping();
        cellMapping.setCell(cell);
        cellMapping.setFieldName(field);
        return addCellMapping(  cellMapping );
    }
    public ExcelFileConfig addCellMapping(int cell,String field,Object defaultValue){
        CellMapping cellMapping = new CellMapping();
        cellMapping.setCell(cell);
        cellMapping.setFieldName(field);
        cellMapping.setDefaultValue(defaultValue);
        return addCellMapping(  cellMapping );
    }
    public ExcelFileConfig addDateCellMapping(int cell,String field,int cellType,Object defaultValue,String dateformat){
        CellMapping cellMapping = new CellMapping();
        cellMapping.setCell(cell);
        cellMapping.setFieldName(field);
        cellMapping.setCellType(cellType);
        cellMapping.setDateFormat(dateformat);
        cellMapping.setDefaultValue(defaultValue);
        return addCellMapping(  cellMapping );
    }
    public ExcelFileConfig addNumberCellMapping(int cell,String field,int cellType,Object defaultValue,String numberformat){
        CellMapping cellMapping = new CellMapping();
        cellMapping.setCell(cell);
        cellMapping.setFieldName(field);
        cellMapping.setCellType(cellType);
        cellMapping.setNumberFormat(numberformat);
        cellMapping.setDefaultValue(defaultValue);
        return addCellMapping(  cellMapping );
    }

    public ExcelFileConfig addDateCellMapping(int cell,String field,int cellType,String dateformat){
        CellMapping cellMapping = new CellMapping();
        cellMapping.setCell(cell);
        cellMapping.setFieldName(field);
        cellMapping.setCellType(cellType);
        cellMapping.setDateFormat(dateformat);
        return addCellMapping(  cellMapping );
    }
    public ExcelFileConfig addNumberCellMapping(int cell,String field,int cellType,String numberformat){
        CellMapping cellMapping = new CellMapping();
        cellMapping.setCell(cell);
        cellMapping.setFieldName(field);
        cellMapping.setCellType(cellType);
        cellMapping.setNumberFormat(numberformat);
        return addCellMapping(  cellMapping );
    }
    public ExcelFileConfig addCellMapping(CellMapping cellMapping ){

        cellMappingList.add(cellMapping);
        cellMappings.put(cellMapping.getCell(),cellMapping);
        return this;
    }

    private int sheet;
    @Override
    public ExcelFileConfig init(){
        super.init();
        this.setEnableInode(false);
        this.setCloseEOF(true);//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化

        return this;
    }
}
