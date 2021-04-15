package org.frameworkset.tran.input.file;

import com.frameworkset.util.SimpleStringUtil;
import org.apache.commons.lang.StringUtils;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.util.OSInfo;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/12
 */
public class FileConfig {
    //文件监听路径
    private String sourcePath;


    //规范路径
    private String normalSourcePath;
    //规范路径
    private Pattern normalSourcePathPattern;
    //文件名称正则匹配
    private String fileNameRegular;
    private Pattern fileNameRexPattern;
    //文件换行标识符，以什么开头,正则匹配
    private String fileHeadLineRegular;
    private Pattern fileHeadLineRexPattern;
    //需要包含的记录条件,正则匹配
    private String[] includeLines;
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

    public void setIgnoreOlderTime(Long ignoreOlderTime) {
        this.ignoreOlderTime = ignoreOlderTime;
    }

    public Long getCloseOlderTime() {
        return closeOlderTime;
    }

    public void setCloseOlderTime(Long closeOlderTime) {
        this.closeOlderTime = closeOlderTime;
    }

    /**
     * If this option is enabled, bboss ignores any files that were modified before the specified timespan.
     * Configuring ignore_older can be especially useful if you keep log files for a long time.
     * For example, if you want to start bboss,
     * but only want to send the newest files and files from last week, you can configure this option.
     *
     * You can use time strings like 2h (2 hours) and 5m (5 minutes). The default is 0,
     * which disables the setting. Commenting out the config has the same effect as setting it to 0.
     */
    private Long ignoreOlderTime ;

    /**
     * If this option is enabled, bboss close any files that were modified before the specified timespan.
     * Configuring closeOlder can be especially useful if you keep log files for a long time.
     * For example, if you want to start bboss,
     * but only want to close the older files and files from last week that be harvesting, you can configure this option.
     *
     * You can use time strings like 2h (2 hours) and 5m (5 minutes). The default is 0,
     * which disables the setting. Commenting out the config has the same effect as setting it to 0.
     */
    private Long closeOlderTime ;
    //是否检测子目录
    private boolean scanChild;
    private FilenameFilter filter ;
    private File logDir;
    /**
     * 需要添加的字段
     */
    private Map<String,Object> addFields;
    /**
     * 需要添加的字段
     */
    private Map<String,Object> ignoreFields;

    public FileConfig(String sourcePath, String fileNameRegular, String fileHeadLineRegular) {
        this.sourcePath = sourcePath;
        normalSourcePath = SimpleStringUtil.getPath(FileInodeHandler.change(sourcePath).toLowerCase(),fileNameRegular);
        this.fileNameRegular = fileNameRegular;
        this.fileHeadLineRegular = fileHeadLineRegular;

    }
    public FileConfig(String sourcePath, String fileNameRegular, String fileHeadLineRegular, boolean scanChild) {
        this.sourcePath = sourcePath;
        normalSourcePath = SimpleStringUtil.getPath(FileInodeHandler.change(sourcePath).toLowerCase(),fileNameRegular);
        this.fileNameRegular = fileNameRegular;
        this.fileHeadLineRegular = fileHeadLineRegular;
        this.scanChild = scanChild;

    }
    public FileConfig addField(String name,Object value){
        if(addFields == null)
            addFields = new HashMap<>();
        addFields.put(name,value);
        return this;
    }

    public Map<String, Object> getAddFields() {
        return addFields;
    }

    public Map<String, Object> getIgnoreFields() {
        return ignoreFields;
    }

    public FileConfig ignoreField(String name){
        if(ignoreFields == null)
            ignoreFields = new HashMap<>();
        ignoreFields.put(name,1);
        return this;
    }
    public String getSourcePath() {
        return sourcePath;
    }

    public FileConfig setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
        return this;
    }

    public String getFileNameRegular() {
        return fileNameRegular;
    }

    public FileConfig setFileNameRegular(String fileNameRegular) {
        this.fileNameRegular = fileNameRegular;
        return this;
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

    public FileConfig setFileHeadLineRegular(String fileHeadLineRegular) {
        this.fileHeadLineRegular = fileHeadLineRegular;
        return this;
    }

    public boolean isScanChild() {
        return scanChild;
    }

    public String getNormalSourcePath() {
        return normalSourcePath;
    }

    public FileConfig setNormalSourcePath(String normalSourcePath) {
        this.normalSourcePath = normalSourcePath;
        return this;
    }

    public String[] getIncludeLines() {
        return includeLines;
    }

    public FileConfig setIncludeLines(String[] includeLines) {
        this.includeLines = includeLines;
        return this;
    }

    public String[] getExcludeLines() {
        return excludeLines;
    }

    public FileConfig setExcludeLines(String[] excludeLines) {
        this.excludeLines = excludeLines;
        return this;
    }

    public int getMaxBytes() {
        return maxBytes;
    }

    public FileConfig setMaxBytes(int maxBytes) {
        this.maxBytes = maxBytes;
        return this;
    }

    public boolean isCloseEOF() {
        return closeEOF;
    }

    public FileConfig setCloseEOF(boolean closeEOF) {
        this.closeEOF = closeEOF;
        return this;
    }

    public Long getStartPointer() {
        return startPointer;
    }

    public FileConfig setStartPointer(Long startPointer) {
        this.startPointer = startPointer;
        return this;
    }

    private boolean inited = false;
    public FileConfig init(){
        if(inited )
            return this;
        inited = true;
        normalSourcePathPattern = Pattern.compile(normalSourcePath);
        if(StringUtils.isNotEmpty(this.fileHeadLineRegular)){
            fileHeadLineRexPattern = Pattern.compile(this.fileHeadLineRegular);
        }
        if(StringUtils.isNotEmpty(this.fileNameRegular)){
            fileNameRexPattern = Pattern.compile(this.fileNameRegular);
        }
        if(includeLines != null && includeLines.length > 0){
            includeLinesRexPattern = new Pattern[includeLines.length];
            for(int i = 0; i < includeLines.length; i ++){
                includeLinesRexPattern[i] = Pattern.compile(this.includeLines[i]);
            }
        }

        if(excludeLines != null && excludeLines.length > 0){
            excludeLinesRexPattern = new Pattern[excludeLines.length];
            for(int i = 0; i < excludeLines.length; i ++){
                excludeLinesRexPattern[i] = Pattern.compile(this.excludeLines[i]);
            }
        }
        logDir  = new File(sourcePath);
        filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                Matcher m = getFileNameRexPattern().matcher(name);
                return m.matches();
//                            return Pattern.matches(fileConfig.getFileNameRegular(), name);
            }
        };
        if(OSInfo.isWindows())
            enableInode = false;
        return this;

    }

    public File getLogDir() {
        return logDir;
    }

    public FilenameFilter getFilter() {
        return filter;
    }

    public Pattern getNormalSourcePathPattern() {
        return normalSourcePathPattern;
    }


    public boolean isEnableInode() {
        return enableInode;
    }
    /**
     * 是否启用inode文件标识符机制来识别文件重命名操作，linux环境下起作用，windows环境下不起作用（enableInode强制为false）
     * linux环境下，在不存在重命名的场景下可以关闭inode文件标识符机制，windows环境下强制关闭inode文件标识符机制
     */
    public FileConfig setEnableInode(boolean enableInode) {
        this.enableInode = enableInode;
        return this;
    }
}
