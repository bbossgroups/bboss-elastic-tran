package org.frameworkset.tran.input.file;

import com.frameworkset.util.BaseSimpleStringUtil;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.record.CommonData;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.util.TranUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/15
 */
public class FileReaderTask {
    private static Logger logger = LoggerFactory.getLogger(FileReaderTask.class);
    /**
     * 文件
     */
    private File file;
    /**
     * 文件号
     */
    private String fileId;
    /**
     * 文件采集偏移量
     */
    private long pointer;
    /**
     * 文件开始行标识，正则表达式
     */
    private String fileHeadLineRegular;
    /**
     * 文件监听事件服务
     */
    private FileListenerService fileListenerService;
    /**
     * 文件开始行标识正则
     */
    private Pattern pattern;
    private boolean rootLevel;
    private boolean enableMeta;
    private String charsetEncode;


    private BaseDataTran fileDataTran;
    private RandomAccessFile raf ;
    //状态 0启用 1失效
    public static final int STATUS_OK = 0;
    public static final int STATUS_NO = 1;
    private int status = STATUS_OK;
    private Status currentStatus;
    private volatile boolean taskEnded;
    /**
     * jsondata：标识文本记录是json格式的数据，true 将值解析为json对象，false - 不解析，这样值将作为一个完整的message字段存放到上报数据中
     */
    private boolean jsondata ;
    private FileConfig fileConfig;
    public FileReaderTask(File file, String fileId, FileConfig fileConfig, FileListenerService fileListenerService, BaseDataTran fileDataTran,Status currentStatus ) {
        this.file = file;
        this.pointer = 0;
        this.fileId = fileId;
        this.fileListenerService = fileListenerService;
        this.fileHeadLineRegular = fileConfig.getFileHeadLineRegular();
        if(fileConfig.getFileHeadLineRexPattern() != null){
            pattern = fileConfig.getFileHeadLineRexPattern() ;
        }
        rootLevel = this.fileListenerService.getFileImportContext().getFileImportConfig().isRootLevel();
        jsondata = this.fileListenerService.getFileImportContext().getFileImportConfig().isJsondata();
        charsetEncode = this.fileListenerService.getFileImportContext().getFileImportConfig().getCharsetEncode();
        enableMeta = this.fileListenerService.getFileImportContext().getFileImportConfig().isEnableMeta();
        this.fileDataTran = fileDataTran;
        this.currentStatus = currentStatus;
        this.fileConfig = fileConfig;

    }
    public FileReaderTask(String fileId,  Status currentStatus ) {
        this.fileId = fileId;

        this.currentStatus = currentStatus;

    }
    public FileReaderTask(File file,String fileId,FileConfig fileConfig,long pointer,FileListenerService fileListenerService, BaseDataTran fileDataTran,Status currentStatus ) {
        this(file,fileId,  fileConfig,fileListenerService,fileDataTran,currentStatus);
        this.pointer = pointer;
    }
    static class Line{
        private String line;
        private boolean eof;

        Line(String line, boolean eof) {
            this.line = line;
            this.eof = eof;
        }

        public String getLine() {
            return line;
        }



        public boolean isEof() {
            return eof;
        }


    }
    /**
     * Reads the next line of text from this file.  This method successively
     * reads bytes from the file, starting at the current file pointer,
     * until it reaches a line terminator or the end
     * of the file.  Each byte is converted into a character by taking the
     * byte's value for the lower eight bits of the character and setting the
     * high eight bits of the character to zero.  This method does not,
     * therefore, support the full Unicode character set.
     *
     * <p> A line of text is terminated by a carriage-return character
     * ({@code '\u005Cr'}), a newline character ({@code '\u005Cn'}), a
     * carriage-return character immediately followed by a newline character,
     * or the end of the file.  Line-terminating characters are discarded and
     * are not included as part of the string returned.
     *
     * <p> This method blocks until a newline character is read, a carriage
     * return and the byte following it are read (to see if it is a newline),
     * the end of the file is reached, or an exception is thrown.
     *
     * @return     the next line of text from this file, or null if end
     *             of file is encountered before even one byte is read.
     * @exception  IOException  if an I/O error occurs.
     */

    public final Line readLine() throws IOException {
        StringBuilder input = new StringBuilder();
        int c = -1;
        boolean eol = false;

        while (!eol) {
            switch (c = raf.read()) {
                case -1:
                case '\n':
                    eol = true;
                    break;
                case '\r':
                    eol = true;
                    long cur = raf.getFilePointer();
                    if ((raf.read()) != '\n') {
                        raf.seek(cur);
                    }
                    break;
                default:
                    input.append((char)c);
                    break;
            }
        }

        if ((c == -1) ) {
            if(input.length() == 0)
                return new Line(null,true);
            else{
                return new Line(input.toString(),true);
            }
        }
        else
            return new Line(input.toString(),false);
    }

    private boolean reachEOFClosed(Line line){
        if(fileConfig.isCloseEOF() && line.isEof()){
            return true;
        }
        return false;
    }
    public void execute() {
        try {
            if(taskEnded)
                return;
            synchronized (this){
                if(raf == null) {
                    RandomAccessFile raf = new RandomAccessFile(file, "r");
                    //文件重新写了，则需要重新读取
                    if(this.pointer > raf.length()){
                        this.pointer = 0;
                        this.currentStatus.setLastValue(0l);
                    }
                    raf.seek(pointer);
                    this.raf = raf;
                }
                //缓存
                StringBuilder builder = new StringBuilder();
                Line line_;
                String line = null;
                List<Record> recordList = new ArrayList<Record>();
                //批量处理记录数
                int fetchSize = this.fileListenerService.getFileImportContext().getFetchSize();
                boolean reachEOFClosed = false;
                while(true){
                    line_ = readLine();
                    reachEOFClosed = reachEOFClosed(line_);
                    if(line_.getLine() != null) {
                        line = line_.getLine();
                        if (charsetEncode != null)
                            line = new String(line.getBytes("ISO-8859-1"), charsetEncode);

                        if (null != pattern) {
                            Matcher m = pattern.matcher(line);
                            if (m.find() && builder.length() > 0) {
                                pointer = raf.getFilePointer();
                                result(file, pointer, builder.toString(), recordList,reachEOFClosed);

                                //分批处理数据
                                if (fetchSize > 0 && ( recordList.size() >= fetchSize)) {
                                    fileDataTran.appendData(new CommonData(recordList));
                                    recordList = new ArrayList<Record>();
                                }

                                builder.setLength(0);
                            }
                            if (builder.length() > 0) {
                                builder.append(TranUtil.lineSeparator);
                            }
                            builder.append(line);
                            if(reachEOFClosed){
                                pointer = raf.getFilePointer();
                                result(file,pointer,builder.toString(), recordList,reachEOFClosed);
                                builder.setLength(0);
                                break;
                            }
                        } else {
                            pointer = raf.getFilePointer();
                            result(file, pointer, line, recordList,reachEOFClosed);
                            //分批处理数据
                            if (fetchSize > 0 && recordList.size() >= fetchSize) {
                                fileDataTran.appendData(new CommonData(recordList));
                                recordList = new ArrayList<Record>();
                            }
                            if(reachEOFClosed)
                                break;

                        }
                    }
                    else{
                        break;
                    }
                }
                if(builder.length() > 0 ){
                    pointer = raf.getFilePointer();
                    result(file,pointer,builder.toString(), recordList,reachEOFClosed);
                    builder.setLength(0);
                    builder = null;
                }
                if(recordList.size() > 0 ){
                    fileDataTran.appendData(new CommonData(recordList));
                }
                //如果设置了文件结束，及结束作业，则进行相应处理
                if(reachEOFClosed){
                    if(logger.isInfoEnabled())
                        logger.info("{} reached eof and will be closed.",toString());
                    this.taskEnded();
                }
            }
        }catch (Exception e){
//            logger.error("",e);
            throw new DataImportException("",e);
        }finally {
            destroy();
        }
    }

    public void destroy(){
        if(raf != null){
            try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            raf = null;
        }
    }

    /**
     * 检查记录内容是否是需要采集的记录内容
     * @param line
     * @return
     */
    private boolean check(String line){
        Pattern[] includes = fileConfig.getIncludeLinesRexPattern();
        Pattern[] excludes = fileConfig.getExcludeLinesRexPattern();
        if(includes != null && includes.length > 0){
            boolean find = false;
            for(Pattern inc:includes){
                find = inc.matcher(line).find();
                if(find)
                    break;
            }
            if(find){
                if(excludes != null && excludes.length > 0){
                    for(Pattern exc:excludes){
                        if(exc.matcher(line).find()){
                            find = false;
                            break;
                        }
                    }
                }
            }
            return find;
        }
        else{
            boolean find = true;
            if(excludes != null && excludes.length > 0){
                for(Pattern exc:excludes){
                    if(exc.matcher(line).find()){
                        find = false;
                        break;
                    }
                }
            }
            return find;
        }

    }

    /**
     * 检查记录内容长度是否超过最大长度，如果超过就需要截取掉超过的内容
     * @param line
     * @return
     */
    private String checkMaxLength(String line){
        int maxLength = fileConfig.getMaxBytes();
        if(line.length() > maxLength){
            line = line.substring(0,maxLength);
        }
        return line;
    }
    private void result(File file, long pointer, String line,List<Record> recordList,boolean reachEOFClosed) {
        if(!check(line)){
            recordList.add(new FileLogRecord(true,pointer,reachEOFClosed));
        }
        else {
            line = checkMaxLength(line);
            Map result = new HashMap();
            try {
                if (jsondata) {
                    //json
                    Map json = SimpleStringUtil.json2Object(line, Map.class);
                    //同级
                    if (rootLevel) {
                        result = json;
                    } else {//不同级
                        result.put("json", json);
                    }
                } else {
                    result.put("@message", line);
                }

            } catch (Exception e) {
                // not json
                result.put("@message", line);
            }
            Map common = common(file, pointer, result);
            if (enableMeta)
                result.put("@filemeta", common);
            recordList.add(new FileLogRecord(common,result,pointer,reachEOFClosed));
        }

    }
    //公共数据
    private Map common(File file, long pointer, Map result) {
        Map common = new HashMap();
        common.put("hostIp", BaseSimpleStringUtil.getIp());
        common.put("hostName",BaseSimpleStringUtil.getHostName());
        common.put("filePath",FileInodeHandler.change(file.getAbsolutePath()));
        common.put("timestamp",new Date());
        common.put("pointer",pointer);
        common.put("fileId",fileId);
        return common;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public void changeFile(File file){
        this.file = file;
        if(currentStatus != null)
            currentStatus.setFilePath(FileInodeHandler.change(file.getAbsolutePath()));
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "{\"file\":\""+ FileInodeHandler.change(file.getAbsolutePath())+"\",\"fileId\":\""+fileId+"\",\"pointer\":"+pointer+"}";
    }

    public boolean isTaskEnded() {
        return taskEnded;
    }

    public void taskEnded() {
        if(taskEnded)
            return;
        synchronized (this) {
            if(taskEnded)
                return;
            this.taskEnded = true;
//            this.currentStatus.setStatus(ImportIncreamentConfig.STATUS_COMPLETE);
            this.fileDataTran.stopTranOnly();
        }
    }
    public BaseDataTran getFileDataTran() {
        return fileDataTran;
    }

}
