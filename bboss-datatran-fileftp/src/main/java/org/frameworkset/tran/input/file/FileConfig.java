package org.frameworkset.tran.input.file;

import org.apache.commons.lang3.StringUtils;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.context.JobContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.input.s3.OSSFileInputConfig;
import org.frameworkset.tran.schedule.timer.TimeRange;
import org.frameworkset.tran.schedule.timer.TimerScheduleConfig;
import org.frameworkset.util.OSInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xutengfei,yinbp
 * @description
 * @create 2021/3/12
 */
public class FileConfig<T extends FileConfig<T>> extends FieldManager<T>{
    private Logger logger = LoggerFactory.getLogger(FileConfig.class);


    private ImportContext importContext;

    //文件监听路径
    private String sourcePath;

    /**
     * ftp配置
     */
    private FtpConfig ftpConfig;


    private OSSFileInputConfig ossFileInputConfig;

    /**
     *重命名文件监听路径：一些日志组件会指定将滚动日志文件放在与当前日志文件不同的目录下，需要通过renameFileSourcePath指定这个不同的目录地址，以便
     * 可以追踪到未采集完毕的滚动日志文件，从而继续采集文件中没有采集完毕的日志
     * 本路径只有在inode机制有效并且启用的情况下才起作用,默认与sourcePath一致
     */


    private String renameFileSourcePath;

    private File renameFileLogDir;

    public TimerScheduleConfig getTimerScheduleConfig() {
        return timerScheduleConfig;
    }

    private TimerScheduleConfig timerScheduleConfig;




    //控制是否删除采集完的文件，默认false 不删除，true 删除
    private boolean deleteEOFFile;
    //规范路径
    //规范路径
    //文件名称正则匹配
    private String fileNameRegular;
    private Pattern fileNameRexPattern;
    public FileConfig(){
        super();
    }
    public int getSkipHeaderLines() {
        return skipHeaderLines;
    }

    public T setSkipHeaderLines(int skipHeaderLines) {
        this.skipHeaderLines = skipHeaderLines;
        return (T)this;
    }

    public T setOssFileInputConfig(OSSFileInputConfig ossFileInputConfig) {
        this.ossFileInputConfig = ossFileInputConfig;
        if(ossFileInputConfig != null){
            this.ossFileInputConfig.setFileConfig(this);
        }
        return (T)this;
    }

    public OSSFileInputConfig getOssFileInputConfig() {
        return ossFileInputConfig;
    }
    public T addField(String name,Object value){
       super.addField(name,value);
       return (T)this;
    }
    public T addFields(Map<String,Object> values){
        super.addFields(values);
        return (T)this;
    }

    /**
     * 忽略文件开始行数
     */
    private int skipHeaderLines;

    public T setFileFilter(FileFilter fileFilter) {
        this.fileFilter = fileFilter;
        return (T)this;
    }

    private FileFilter fileFilter;
    //文件换行标识符，以什么开头,正则匹配
    private String fileHeadLineRegular;
    private Pattern fileHeadLineRexPattern;
    //需要包含的记录条件,正则匹配
    private String[] includeLines;
    private String charsetEncode ;
    public LineMatchType getIncludeLineMatchType() {
        return includeLineMatchType;
    }

    private LineMatchType includeLineMatchType = LineMatchType.REGEX_CONTAIN;
    private Pattern[] includeLinesRexPattern;
    //需要排除的记录规则,正则匹配
    /**
     * If both include_lines and exclude_lines are defined,
     * bboss executes include_lines first and then executes exclude_lines.
     * The order in which the two options are defined doesn’t matter.
     * The include_lines option will always be executed before the exclude_lines option,
     * even if exclude_lines appears before include_lines in the config file.
     */
    private String[] excludeLines;

    public LineMatchType getExcludeLineMatchType() {
        return excludeLineMatchType;
    }

    private LineMatchType excludeLineMatchType = LineMatchType.REGEX_CONTAIN;
    private Pattern[] excludeLinesRexPattern;
    /**
     * The maximum number of bytes that a single log message can have. All bytes after max_bytes are discarded and not sent.
     * This setting is especially useful for multiline log messages, which can get large. The default is 1MB (1048576).
     */
    private int maxBytes = 1048576;
    /**
     * When this option is enabled, bboss closes a file as soon as the end of a file is reached. This is useful when your files are only written once and not updated from time to time. For example,
     * this happens when you are writing every single log event to a new file. This option is disabled by default.
     */
    private boolean closeEOF ;

	/**
	 * 重命名后的文件采集完毕后，是否要被关闭,默认true
	 * true 关闭
	 * false 不关闭
	 */
	private boolean closeRenameEOF = true;



    /**
     *  指定开始采集位置
     */
    private Long startPointer;
    /**
     * 是否启用inode文件标识符机制来识别文件重命名操作，linux环境下起作用，windows环境下不起作用（enableInode强制为false）
     * linux环境下，在不存在重命名的场景下可以关闭inode文件标识符机制，windows环境下强制关闭inode文件标识符机制
     */
    private boolean enableInode = true;

    public Long getIgnoreOlderTime() {
        return ignoreOlderTime;
    }

    public T setIgnoreOlderTime(Long ignoreOlderTime) {
        this.ignoreOlderTime = ignoreOlderTime;
        return (T)this;
    }

    public Long getCloseOlderTime() {
        return closeOlderTime;
    }

    public T setCloseOlderTime(Long closeOlderTime) {
        this.closeOlderTime = closeOlderTime;
        return (T)this;
    }

    /**
     * 作业启动时起作用
     *
     * If this option is enabled, bboss ignores any files that were modified before the specified timespan.
     * Configuring ignore_older can be especially useful if you keep log files for a long time.
     * For example, if you want to start bboss,
     * but only want to send the newest files and files from last week, you can configure this option.
     *
     * You can use time strings like 2h (2 hours) and 5m (5 minutes). The default is 0,
     * which disables the setting. Commenting out the config has the same effect as setting it to 0.
     */
    private Long ignoreOlderTime ;

    private IgnoreFileAssert ignoreFileAssert;

    /**
     * 允许文件内容静默最大时间，单位毫秒，如果在idleMaxTime访问内一直没有数据更新，认为文件是静默文件，将不再采集静默文件数据
     * If this option is enabled, bboss close any files that were modified before the specified timespan.
     * Configuring closeOlder can be especially useful if you keep log files for a long time.
     * For example, if you want to start bboss,
     * but only want to close the older files and files from last week that be harvesting, you can configure this option.
     *
     * You can use time strings like 2h (2 hours) and 5m (5 minutes). The default is 0,
     * which disables the setting. Commenting out the config has the same effect as setting it to 0.
     */
    private Long closeOlderTime ;
    private CloseOldedFileAssert closeOldedFileAssert;
    /**
     * 是否检测子目录
     * 如果扫描子目录，则inode机制强制关闭
     */

    private boolean scanChild;
    private FilenameFilter filter ;
    private File logDir;



    public FieldBuilder getFieldBuilder() {
        return fieldBuilder;
    }

    public T setFieldBuilder(FieldBuilder fieldBuilder) {
        this.fieldBuilder = fieldBuilder;
        return (T)this;
    }

    private FieldBuilder fieldBuilder;


    public FileConfig(String sourcePath, String fileNameRegular, String fileHeadLineRegular) {
        this.sourcePath = FileInodeHandler.change(sourcePath);
        this.fileNameRegular = fileNameRegular;
        this.fileHeadLineRegular = fileHeadLineRegular;

    }

    public FileConfig(String sourcePath, String fileNameRegular, String fileHeadLineRegular, boolean scanChild) {
        this.sourcePath = FileInodeHandler.change(sourcePath);
        this.fileNameRegular = fileNameRegular;
        this.fileHeadLineRegular = fileHeadLineRegular;
        this.scanChild = scanChild;

    }




    public T setScanChild(boolean scanChild) {
        this.scanChild = scanChild;
        return (T)this;
    }

    public FileConfig(String sourcePath, FileFilter fileFilter, String fileHeadLineRegular) {
        this.sourcePath = FileInodeHandler.change(sourcePath);
        this.fileFilter = fileFilter;
        this.fileHeadLineRegular = fileHeadLineRegular;

    }
    public FileConfig(String sourcePath, FileFilter fileFilter, String fileHeadLineRegular, boolean scanChild) {
        this.sourcePath = FileInodeHandler.change(sourcePath);
        this.fileFilter = fileFilter;
        this.fileHeadLineRegular = fileHeadLineRegular;
        this.scanChild = scanChild;

    }


    public String getSourcePath() {
        return sourcePath;
    }

    public T setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
        return (T)this;
    }

    public String getFileNameRegular() {
        return fileNameRegular;
    }

    public T setFileNameRegular(String fileNameRegular) {
        this.fileNameRegular = fileNameRegular;
        return (T)this;
    }

    public Pattern getFileHeadLineRexPattern() {
        return fileHeadLineRexPattern;
    }

    public Pattern getFileNameRexPattern() {
        return fileNameRexPattern;
    }

    public Pattern[] getExcludeLinesRexPattern() {
        return excludeLinesRexPattern;
    }

    public Pattern[] getIncludeLinesRexPattern() {
        return includeLinesRexPattern;
    }

    public String getFileHeadLineRegular() {
        return fileHeadLineRegular;
    }

    public T setFileHeadLineRegular(String fileHeadLineRegular) {
        this.fileHeadLineRegular = fileHeadLineRegular;
        return (T)this;
    }

    public boolean isScanChild() {
        return scanChild;
    }



    public String[] getIncludeLines() {
        return includeLines;
    }

    public T setIncludeLines(String[] includeLines,LineMatchType includeLineMatchType) {
        this.includeLines = includeLines;
        this.includeLineMatchType = includeLineMatchType;
        return (T)this;
    }
    public T setIncludeLines(String[] includeLines) {
        this.includeLines = includeLines;
        return (T)this;
    }
    public String[] getExcludeLines() {
        return excludeLines;
    }
    public T setExcludeLines(String[] excludeLines) {
        this.excludeLines = excludeLines;
        return (T)this;
    }

    public T setExcludeLines(String[] excludeLines,LineMatchType excludeLineMatchType) {
        this.excludeLines = excludeLines;
        this.excludeLineMatchType = excludeLineMatchType;
        return (T)this;
    }

    public int getMaxBytes() {
        return maxBytes;
    }

    public T setMaxBytes(int maxBytes) {
        this.maxBytes = maxBytes;
        return (T)this;
    }

    public boolean isCloseEOF() {
        return closeEOF;
    }

    public T setCloseEOF(boolean closeEOF) {
        this.closeEOF = closeEOF;
        return (T)this;
    }

    public Long getStartPointer() {
        return startPointer;
    }

    public T setStartPointer(Long startPointer) {
        this.startPointer = startPointer;
        return (T)this;
    }

    private boolean inited = false;
    public void destroy(){
        if(ftpConfig != null){
            ftpConfig.destroy();
        }
        if(ossFileInputConfig != null){
            ossFileInputConfig.destroy();
        }
//        importContext = null;
    }
    public T init(){
        if(inited )
            return (T)this;
        build();
        inited = true;
        if(getCloseOlderTime() != null && getCloseOlderTime() > 0L) {
            logger.info("CloseOlderTime = {}, setCloseEOF(false)",getCloseOlderTime() );
            setCloseEOF(false);//指定CloseOlderTime
        }
        else if(getIgnoreOlderTime() != null && getIgnoreOlderTime() > 0L) {
            logger.info("getIgnoreOlderTime = {}, setCloseEOF(false)",getIgnoreOlderTime() );
            setCloseEOF(false);//指定CloseOlderTime
        }

        if(sourcePath == null || sourcePath.equals("")){
            throw new FilelogPluginException("sourcePath is null or empty.");
        }
        else{
            if(renameFileSourcePath == null || renameFileSourcePath.equals("")){
                renameFileSourcePath = sourcePath;
            }
        }
        logDir  = new File(sourcePath);
        if(renameFileSourcePath != null ){
            renameFileLogDir = new File(renameFileSourcePath);
        }
        if(StringUtils.isNotEmpty(this.fileHeadLineRegular)){
            fileHeadLineRexPattern = Pattern.compile(this.fileHeadLineRegular);
        }
        if(StringUtils.isNotEmpty(this.fileNameRegular)){
            fileNameRexPattern = Pattern.compile(this.fileNameRegular);
        }
        if(includeLines != null && includeLines.length > 0
                && (includeLineMatchType == LineMatchType.REGEX_CONTAIN
                || includeLineMatchType == LineMatchType.REGEX_MATCH)){
            includeLinesRexPattern = new Pattern[includeLines.length];
            for(int i = 0; i < includeLines.length; i ++){
                includeLinesRexPattern[i] = Pattern.compile(this.includeLines[i]);
            }
        }

        if(excludeLines != null && excludeLines.length > 0
                && (excludeLineMatchType == LineMatchType.REGEX_CONTAIN
                    || excludeLineMatchType == LineMatchType.REGEX_MATCH)){
            excludeLinesRexPattern = new Pattern[excludeLines.length];
            for(int i = 0; i < excludeLines.length; i ++){
                excludeLinesRexPattern[i] = Pattern.compile(this.excludeLines[i]);
            }
        }
        filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {

                if(!isScanChild() ) {
                    boolean isSameDir = logDir.getAbsolutePath().equals(dir.getAbsolutePath());
                    if (!isSameDir)//文件路径不匹配，直接忽略
                        return false;
                }
                if(fileNameRexPattern != null) {
                    Matcher m = fileNameRexPattern.matcher(name);
                    return m.matches();
                }
                else if(fileFilter != null){
                    try {
                        return fileFilter.accept(new LocalFilterFileInfo(dir, name), FileConfig.this);
                    }
                    catch (DataImportException e){
                        logger.warn(name,e);
                        throw e;
                    }
                    catch (Exception e){
//                        logger.warn(name,e);
                        throw ImportExceptionUtil.buildDataImportException(importContext.getOutputPlugin(),importContext,name,e);
                    }

                }
                /**
                 * 默认接收所有文件
                 */
                return true;
            }
        };
        if(OSInfo.isWindows())
            enableInode = false;
        if(ftpConfig != null){
            ftpConfig.init(this);

        }
        else if(ossFileInputConfig != null){
            ossFileInputConfig.init(this);
        }

        return (T)this;

    }


    public File getLogDir() {
        return logDir;
    }

    public FilenameFilter getFilter() {
        return filter;
    }



    public boolean isEnableInode() {
        return enableInode;
    }
    /**
     * 是否启用inode文件标识符机制来识别文件重命名操作，linux环境下起作用，windows环境下不起作用（enableInode强制为false）
     * linux环境下，在不存在重命名的场景下可以关闭inode文件标识符机制，windows环境下强制关闭inode文件标识符机制
     */
    public T setEnableInode(boolean enableInode) {
        this.enableInode = enableInode;
        return (T)this;
    }

    public boolean isDeleteEOFFile() {
        return deleteEOFFile;
    }

    public T setDeleteEOFFile(boolean deleteEOFFile) {
        this.deleteEOFFile = deleteEOFFile;
        return (T)this;
    }

    public String getCharsetEncode() {
        return charsetEncode;
    }

    public T setCharsetEncode(String charsetEncode) {
        this.charsetEncode = charsetEncode;
        return (T)this;
    }

    public FileFilter getFileFilter() {
        return fileFilter;
    }

    public boolean checkFilePath(String filePath){
        if(filter != null){
            File file = new File(filePath);
            return filter.accept(file.getParentFile(),file.getName());
        }
        else{
            return false;
        }
    }


    public IgnoreFileAssert getIgnoreFileAssert() {
        return ignoreFileAssert;
    }

    public T setIgnoreFileAssert(IgnoreFileAssert ignoreFileAssert) {
        this.ignoreFileAssert = ignoreFileAssert;
        return (T)this;
    }

    public CloseOldedFileAssert getCloseOldedFileAssert() {
        return closeOldedFileAssert;
    }

    public T setCloseOldedFileAssert(CloseOldedFileAssert closeOldedFileAssert) {
        this.closeOldedFileAssert = closeOldedFileAssert;
        return (T)this;
    }

    public T setRenameFileSourcePath(String renameFileSourcePath) {
        this.renameFileSourcePath = renameFileSourcePath;
        return (T)this;
    }

    public File getRenameFileLogDir() {
        return renameFileLogDir;
    }

    public String getRenameFileSourcePath() {
        return renameFileSourcePath;
    }

	public boolean isCloseRenameEOF() {
		return closeRenameEOF;
	}

	public T setCloseRenameEOF(boolean closeRenameEOF) {
		this.closeRenameEOF = closeRenameEOF;
		return (T)this;
	}
    /**
     * 添加不扫码新文件的时间段
     * timeRange必须是以下三种类型格式
     * 11:30-12:30  每天在11:30和12:30之间运行
     * 11:30-    每天11:30开始执行,到23:59结束
     * -12:30    每天从00:00开始到12:30
     * @param timeRange
     * @return
     */
    public T addSkipScanNewFileTimeRange(String timeRange){
        if(timerScheduleConfig == null){
            timerScheduleConfig = new TimerScheduleConfig();
        }
        timerScheduleConfig.addSkipScanNewFileTimeRange(timeRange);
        return (T)this;
    }

    /**
     * 添加扫码新文件的时间段，每天扫描新文件时间段，优先级高于不扫码时间段，先计算是否在扫描时间段，如果是则扫描，不是则不扫码
     * timeRange必须是以下三种类型格式
     * 11:30-12:30  每天在11:30和12:30之间运行
     * 11:30-    每天11:30开始执行,到23:59结束
     * -12:30    每天从00:00开始到12:30
     * @param timeRange
     * @return
     */
    public T addScanNewFileTimeRange(String timeRange){
        if(timerScheduleConfig == null){
            timerScheduleConfig = new TimerScheduleConfig();
        }
        timerScheduleConfig.addSkipScanNewFileTimeRange(timeRange);

        return (T)this;
    }



    public List<TimeRange> getScanNewFileTimeRanges() {
        return timerScheduleConfig != null?timerScheduleConfig.getScanNewFileTimeRanges():null;
    }

    public List<TimeRange> getSkipScanNewFileTimeRanges() {
        return timerScheduleConfig != null?timerScheduleConfig.getSkipScanNewFileTimeRanges():null;
    }
    @Override
    public String toString(){
        StringBuilder ret = new StringBuilder();
        buildMsg(ret);
        return ret.toString();

    }


    protected void buildMsg(StringBuilder stringBuilder){
        stringBuilder.append("sourcePath:").append(this.sourcePath);
        stringBuilder.append(",skipHeaderLines:").append(this.skipHeaderLines);
        if(ftpConfig != null){
            ftpConfig.buildMsg(stringBuilder);
        }
        appendFieldList( stringBuilder);
    }

    public FtpConfig getFtpConfig() {
        return ftpConfig;
    }

    public T setFtpConfig(FtpConfig ftpConfig) {
        this.ftpConfig = ftpConfig;
        if(ftpConfig != null)
            ftpConfig.setFileConfig(this);
        return (T)this;
    }


    public void setImportContext(ImportContext importContext) {
        this.importContext = importContext;
    }

    public ImportContext getImportContext() {
        return importContext;
    }
    public JobContext getJobContext(){
        if(importContext != null) {
            return importContext.getJobContext();
        }
        else{
            return null;
        }
    }

    public Object getJobData(String name){
        return getJobContext().getJobData(name);
    }

    public void build(){

    }
}
