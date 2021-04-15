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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Thread.sleep;

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
    private String filePath;

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
    private Thread worker ;
    private long oldLastModifyTime = -1;
    private long checkFileModifyInterval = 3000l;
    public FileReaderTask(File file, String fileId, FileConfig fileConfig, FileListenerService fileListenerService, BaseDataTran fileDataTran,
                          Status currentStatus ) {
        this.file = file;
        this.filePath = FileInodeHandler.change(file.getAbsolutePath());
        this.pointer = 0;
        this.fileId = fileId;
        this.fileListenerService = fileListenerService;
        if(fileConfig.getFileHeadLineRexPattern() != null){
            pattern = fileConfig.getFileHeadLineRexPattern() ;
        }
        rootLevel = this.fileListenerService.getFileImportContext().getFileImportConfig().isRootLevel();
        jsondata = this.fileListenerService.getFileImportContext().getFileImportConfig().isJsondata();
        charsetEncode = this.fileListenerService.getFileImportContext().getFileImportConfig().getCharsetEncode();
        enableMeta = this.fileListenerService.getFileImportContext().getFileImportConfig().isEnableMeta();
        checkFileModifyInterval = this.fileListenerService.getFileImportContext().getFileImportConfig().getCheckFileModifyInterval();
        this.fileDataTran = fileDataTran;
        this.currentStatus = currentStatus;
        this.fileConfig = fileConfig;


    }
    public FileReaderTask(String fileId,  Status currentStatus ) {
        this.fileId = fileId;

        this.currentStatus = currentStatus;

    }

    /**
     * 检测文件是否被重命名，如果重命名则标识文件还存在
     * 1 文件被删除
     * 2 文件被重命名
     * -1 未知状态  判断过程中出错了，返回未知状态
     * @param logFileId
     * @return
     */
    public int fileExist(String logFileId){
        if(!fileConfig.isEnableInode()){
            return 1;
        }
        File logDir = fileConfig.getLogDir();
        FilenameFilter filter = fileConfig.getFilter();
        try {
            if (logDir.isDirectory() && logDir.exists()) {
                File[] files = logDir.listFiles(filter);
                File file = null;
                for (int i = 0; files != null && i < files.length; i++) {
                    file = files[i];
                    String fileId = FileInodeHandler.inode(file,fileConfig.isEnableInode());
                    if(fileId.equals(logFileId) ){
                        //文件存在，被重命名了
                        return 2;
                    }
                }
            }
            //文件被删除了
            return 1;
        }
        catch(Exception e){
            //判断过程中出错了，返回未知状态
            return -1;
        }

    }

    /**
     * 判断文件是否被重命名
     * @param logFile
     * @return
     */
    public boolean fileRenamed(File logFile){
        if(!fileConfig.isEnableInode()){
            return false;
        }
        String logFileId = FileInodeHandler.inode(logFile,fileConfig.isEnableInode());
        return  !fileId.equals(logFileId);

    }

    public FileReaderTask(File file, String fileId, FileConfig fileConfig, long pointer, FileListenerService fileListenerService, BaseDataTran fileDataTran,
                          Status currentStatus   ) {
        this(file,fileId,  fileConfig,fileListenerService,fileDataTran,currentStatus);
        this.pointer = pointer;
    }
    public void start(){
        worker = new Thread(new Work(),"FileReaderTask-Thread");
//        worker.setDaemon(true);
        worker.start();
    }
    public String getFilePath() {
        return filePath;
    }
    public boolean isEnableInode(){
        return fileConfig.isEnableInode();
    }
    class Work implements Runnable{

        @Override
        public void run() {
            boolean delete = false;
            do {
                if(taskEnded || fileListenerService.getBaseDataTranPlugin().checkTranToStop()){
                    break;
                }
                if(file.exists()){

                    long lastModifyTime = file.lastModified();
                    if(oldLastModifyTime == -1){
                        oldLastModifyTime = lastModifyTime;
                        execute();
                        continue;
                    }
                    else if(oldLastModifyTime == lastModifyTime){
                        try {
                            sleep(checkFileModifyInterval);
                        } catch (InterruptedException e) {
                            break;
                        }
                        continue;
                    }
                    else{
                        if(fileRenamed(file))
                        {
                            try {
                                sleep(checkFileModifyInterval);
                            } catch (InterruptedException e) {
                                break;
                            }
                            continue;
                        }
                        oldLastModifyTime = lastModifyTime;
                        execute();
                        continue;
                    }
                }
                else{ //可能的删除文件，待处理
                    int lable = fileExist(fileId);//根据文件号识别文件是否被删除
                    if(lable == 1)//文件被删除
                    {
                        fileListenerService.doDelete(fileId);
                        delete = true;
                        break;
                    }
                    try {
                        sleep(checkFileModifyInterval);
                    } catch (InterruptedException e) {
                        break;
                    }
                    continue;
                }



            }while(true);
            if(delete){
                taskEnded();
            }
        }

    }
    class Line{
        private String line;
        private boolean eof;
        private boolean eol;
        Line(String line, boolean eof,boolean eol) {
            this.line = line;
            this.eof = eof;
            this.eol = eol;
        }

        public String getLine() {
            return line;
        }



        public boolean isEof() {
            return eof;
        }

        public boolean isRollbackPreLine(){
            return eof && !eol && !fileConfig.isCloseEOF();
        }


    }
    public Status getCurrentStatus() {
        return currentStatus;
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

    public final Line readLine(long startPointer) throws IOException {
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
                return new Line(null,true,true);
            else{
                if(fileConfig.isCloseEOF())
                    return new Line(input.toString(),true,false);
                else{ // 需要结束本次采集
                    raf.seek(startPointer);
                    return new Line(null,true,false);
                }
            }
        }
        else
            return new Line(input.toString(),false,eol);
    }

    private boolean reachEOFClosed(Line line){
        if(fileConfig.isCloseEOF() && line.isEof()){
            return true;
        }
        return false;
    }
    private void execute() {
        try {
            if(taskEnded)
                return;
//            synchronized (this){  单线程处理，无需同步处理
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
                long startPointer = pointer;
                while(true){
                    line_ = readLine( startPointer);
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
                                startPointer =  pointer;
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
                                startPointer =  pointer;

                                builder.setLength(0);
                                break;
                            }
                        } else {
                            pointer = raf.getFilePointer();
                            result(file, pointer, line, recordList,reachEOFClosed);
                            startPointer =  pointer;
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
                    if(!line_.isRollbackPreLine()) {
                        pointer = raf.getFilePointer();
                        result(file, pointer, builder.toString(), recordList, reachEOFClosed);
                    }
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
                    fileListenerService.moveTaskToComplete(this);
                    this.taskEnded();
                }
//            }
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

            Map result = new HashMap();
            try {
                if (jsondata) {
                    //json
                    Map json = SimpleStringUtil.json2Object(line, Map.class);
                    Map addFields = fileConfig.getAddFields();
                    if(addFields != null && addFields.size() > 0){
                        json.putAll(addFields);
                    }
                    Map<String, Object> ignoreFields = fileConfig.getIgnoreFields();
                    if(ignoreFields != null && ignoreFields.size() > 0){
                        Iterator iterator = ignoreFields.keySet().iterator();
                        while (iterator.hasNext()){
                            json.remove(iterator.next());
                        }
                    }
                    //同级
                    if (rootLevel) {

                        result = json;
                    } else {//不同级
                        result.put("json", json);
                    }
                } else {
                    line = checkMaxLength(line);
                    result.put("@message", line);
                    Map addFields = fileConfig.getAddFields();
                    if(addFields != null && addFields.size() > 0){
                        result.putAll(addFields);
                    }
                }

            } catch (Exception e) {
                // not json
                result.put("@message", line);
                Map addFields = fileConfig.getAddFields();
                if(addFields != null && addFields.size() > 0){
                    result.putAll(addFields);
                }
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
