package org.frameworkset.tran.schedule.timer;

import org.frameworkset.tran.schedule.ScheduleConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 节假日调度配置类
 * 用于指定星期六、星期天及特定节假日不执行调度任务
 * <p>Copyright (c) 2025</p>
 * @Date 2025/6/8
 * @author biaoping.yin
 * @version 1.0
 */
public class HolidayScheduleConfig extends ScheduleConfig {

	/**
	 * 是否跳过星期六
	 */
	private boolean skipSaturday;
	/**
	 * 是否跳过星期天
	 */
	private boolean skipSunday;
	/**
	 * 是否跳过元旦（1月1日）
	 */
	private boolean skipNewYearsDay;
	/**
	 * 是否跳过劳动节（5月1日）
	 */
	private boolean skipLaborDay;
	/**
	 * 是否跳过端午节
	 */
	private boolean skipDragonBoatFestival;
	/**
	 * 是否跳过中秋节
	 */
	private boolean skipMidAutumnFestival;
	/**
	 * 是否跳过国庆节（10月1日）
	 */
	private boolean skipNationalDay;
	/**
	 * 是否跳过春节
	 */
	private boolean skipSpringFestival;
	/**
	 * 自定义节假日日期列表，格式：yyyy-MM-dd
	 */
	private List<String> customHolidays;

    public HolidayScheduleConfig(){
        super();
        try {
            init();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
	public boolean isSkipSaturday() {
		return skipSaturday;
	}

	public HolidayScheduleConfig setSkipSaturday(boolean skipSaturday) {
		this.skipSaturday = skipSaturday;
        return this;
	}

	public boolean isSkipSunday() {
		return skipSunday;
	}

	public HolidayScheduleConfig setSkipSunday(boolean skipSunday) {
		this.skipSunday = skipSunday;
        return this;
	}

	public boolean isSkipNewYearsDay() {
		return skipNewYearsDay;
	}

	public HolidayScheduleConfig setSkipNewYearsDay(boolean skipNewYearsDay) {
		this.skipNewYearsDay = skipNewYearsDay;
        return this;
	}

	public boolean isSkipLaborDay() {
		return skipLaborDay;
	}

	public HolidayScheduleConfig setSkipLaborDay(boolean skipLaborDay) {
		this.skipLaborDay = skipLaborDay;
        return this;
	}

	public boolean isSkipDragonBoatFestival() {
		return skipDragonBoatFestival;
	}

	public HolidayScheduleConfig setSkipDragonBoatFestival(boolean skipDragonBoatFestival) {
		this.skipDragonBoatFestival = skipDragonBoatFestival;
        return this;
	}

	public boolean isSkipMidAutumnFestival() {
		return skipMidAutumnFestival;
	}

	public HolidayScheduleConfig setSkipMidAutumnFestival(boolean skipMidAutumnFestival) {
		this.skipMidAutumnFestival = skipMidAutumnFestival;
        return this;
	}

	public boolean isSkipNationalDay() {
		return skipNationalDay;
	}

	public HolidayScheduleConfig setSkipNationalDay(boolean skipNationalDay) {
		this.skipNationalDay = skipNationalDay;
        return this;
	}

	public boolean isSkipSpringFestival() {
		return skipSpringFestival;
	}

	public HolidayScheduleConfig setSkipSpringFestival(boolean skipSpringFestival) {
		this.skipSpringFestival = skipSpringFestival;
        return this;
	}

	public List<String> getCustomHolidays() {
		return customHolidays;
	}

	public HolidayScheduleConfig setCustomHolidays(List<String> customHolidays) {
		this.customHolidays = customHolidays;
        return this;
	}

	/**
	 * 设置跳过星期六
	 * @return
	 */
	public HolidayScheduleConfig skipSaturday(){
		this.skipSaturday = true;
		return this;
	}

	/**
	 * 设置跳过星期天
	 * @return
	 */
	public HolidayScheduleConfig skipSunday(){
		this.skipSunday = true;
		return this;
	}

	/**
	 * 设置跳过周末（星期六和星期天）
	 * @return
	 */
	public HolidayScheduleConfig skipWeekends(){
		this.skipSaturday = true;
		this.skipSunday = true;
		return this;
	}

	/**
	 * 设置跳过元旦（1月1日）
	 * @return
	 */
	public HolidayScheduleConfig skipNewYearsDay(){
		this.skipNewYearsDay = true;
		return this;
	}

	/**
	 * 设置跳过劳动节（5月1日）
	 * @return
	 */
	public HolidayScheduleConfig skipLaborDay(){
		this.skipLaborDay = true;
		return this;
	}

	/**
	 * 设置跳过端午节
	 * @return
	 */
	public HolidayScheduleConfig skipDragonBoatFestival(){
		this.skipDragonBoatFestival = true;
		return this;
	}

	/**
	 * 设置跳过中秋节
	 * @return
	 */
	public HolidayScheduleConfig skipMidAutumnFestival(){
		this.skipMidAutumnFestival = true;
		return this;
	}

	/**
	 * 设置跳过国庆节（10月1日）
	 * @return
	 */
	public HolidayScheduleConfig skipNationalDay(){
		this.skipNationalDay = true;
		return this;
	}

	/**
	 * 设置跳过春节
	 * @return
	 */
	public HolidayScheduleConfig skipSpringFestival(){
		this.skipSpringFestival = true;
		return this;
	}

	/**
	 * 设置跳过所有内置节假日（元旦、劳动节、端午节、中秋节、国庆节、春节）及周末
	 * @return
	 */
	public HolidayScheduleConfig skipAllHolidays(){
		this.skipNewYearsDay = true;
		this.skipLaborDay = true;
		this.skipDragonBoatFestival = true;
		this.skipMidAutumnFestival = true;
		this.skipNationalDay = true;
		this.skipSpringFestival = true;
        this.skipWeekends();
		return this;
	}

	/**
	 * 添加自定义节假日日期
	 * @param holidayDate 格式：yyyy-MM-dd
	 * @return
	 */
	public HolidayScheduleConfig addCustomHoliday(String holidayDate){
		if(customHolidays == null){
			customHolidays = new ArrayList<>();
		}
		if(holidayDate != null && !holidayDate.trim().equals("")){
			customHolidays.add(holidayDate.trim());
		}
		return this;
	}

    private static final String DATE_FORMAT = "yyyy-MM-dd";

    /**
     * 春节放假区间映射，key: 年份，value: [开始日期, 结束日期] 格式 yyyy-MM-dd
     * 放假9天
     */
    private Map<Integer, Date[]> SPRING_FESTIVAL_RANGE_MAP ;

    /**
     * 端午节放假区间映射，key: 年份，value: [开始日期, 结束日期] 格式 yyyy-MM-dd
     * 放假3天
     */
    private  Map<Integer, Date[]> DRAGON_BOAT_FESTIVAL_RANGE_MAP ;

    /**
     * 中秋节放假区间映射，key: 年份，value: [开始日期, 结束日期] 格式 yyyy-MM-dd
     * 放假3天
     */
    private   Map<Integer, Date[]> MID_AUTUMN_FESTIVAL_RANGE_MAP  ;

    /**
     * 元旦放假区间映射，key: 年份，value: [开始日期, 结束日期] 格式 yyyy-MM-dd
     * 放假3天
     */
    private   Map<Integer, Date[]> NEW_YEARS_DAY_RANGE_MAP ;

    /**
     * 劳动节放假区间映射，key: 年份，value: [开始日期, 结束日期] 格式 yyyy-MM-dd
     * 放假5天
     */
    private   Map<Integer, Date[]> LABOR_DAY_RANGE_MAP ;

    /**
     * 国庆节放假区间映射，key: 年份，value: [开始日期, 结束日期] 格式 yyyy-MM-dd
     * 放假7天
     */
    private   Map<Integer, Date[]> NATIONAL_DAY_RANGE_MAP  ;

    public void init() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SPRING_FESTIVAL_RANGE_MAP = new HashMap<Integer, Date[]>() {{
            put(2025, new Date[]{sdf.parse("2025-01-28"), sdf.parse("2025-02-05")});
            put(2026, new Date[]{sdf.parse("2026-02-16"), sdf.parse("2026-02-24")});
            put(2027, new Date[]{sdf.parse("2027-02-05"), sdf.parse("2027-02-13")});
            put(2028, new Date[]{sdf.parse("2028-01-25"), sdf.parse("2028-02-02")});
            put(2029, new Date[]{sdf.parse("2029-02-12"), sdf.parse("2029-02-20")});
            put(2030, new Date[]{sdf.parse("2030-02-02"), sdf.parse("2030-02-10")});
            put(2031, new Date[]{sdf.parse("2031-01-22"), sdf.parse( "2031-01-30)")});
            put(2032, new Date[]{sdf.parse("2032-02-10"), sdf.parse("2032-02-18")});
            put(2033, new Date[]{sdf.parse("2033-01-30"), sdf.parse("2033-02-07")});
            put(2034, new Date[]{sdf.parse("2034-02-18"), sdf.parse("2034-02-26")});
            put(2035, new Date[]{sdf.parse("2035-02-07"), sdf.parse("2035-02-15")});
        }};

        /**
         * 端午节放假区间映射，key: 年份，value: [开始日期, 结束日期] 格式 yyyy-MM-dd
         * 放假3天
         */
        DRAGON_BOAT_FESTIVAL_RANGE_MAP = new HashMap<Integer, Date[]>() {{
            put(2025, new Date[]{sdf.parse("2025-05-31"), sdf.parse("2025-06-02")});
            put(2026, new Date[]{sdf.parse("2026-06-19"), sdf.parse("2026-06-21")});
            put(2027, new Date[]{sdf.parse("2027-06-09"), sdf.parse("2027-06-11")});
            put(2028, new Date[]{sdf.parse("2028-05-27"), sdf.parse("2028-05-29")});
            put(2029, new Date[]{sdf.parse("2029-06-15"), sdf.parse("2029-06-17")});
            put(2030, new Date[]{sdf.parse("2030-06-05"), sdf.parse("2030-06-07")});
            put(2031, new Date[]{sdf.parse("2031-05-26"), sdf.parse("2031-05-28")});
            put(2032, new Date[]{sdf.parse("2032-06-12"), sdf.parse("2032-06-14")});
            put(2033, new Date[]{sdf.parse("2033-06-01"), sdf.parse("2033-06-03")});
            put(2034, new Date[]{sdf.parse("2034-06-20"), sdf.parse("2034-06-22")});
            put(2035, new Date[]{sdf.parse("2035-06-10"), sdf.parse("2035-06-12")});
        }};

        /**
         * 中秋节放假区间映射，key: 年份，value: [开始日期, 结束日期] 格式 yyyy-MM-dd
         * 放假3天
         */
       MID_AUTUMN_FESTIVAL_RANGE_MAP = new HashMap<Integer, Date[]>() {{
            put(2025, new Date[]{sdf.parse("2025-10-06"), sdf.parse("2025-10-08")});
            put(2026, new Date[]{sdf.parse("2026-09-25"), sdf.parse("2026-09-27")});
            put(2027, new Date[]{sdf.parse("2027-09-15"), sdf.parse("2027-09-17")});
            put(2028, new Date[]{sdf.parse("2028-10-03"), sdf.parse("2028-10-05")});
            put(2029, new Date[]{sdf.parse("2029-09-22"), sdf.parse("2029-09-24")});
            put(2030, new Date[]{sdf.parse("2030-09-12"), sdf.parse("2030-09-14")});
            put(2031, new Date[]{sdf.parse("2031-10-01"), sdf.parse("2031-10-03")});
            put(2032, new Date[]{sdf.parse("2032-09-19"), sdf.parse("2032-09-21")});
            put(2033, new Date[]{sdf.parse("2033-09-08"), sdf.parse("2033-09-10")});
            put(2034, new Date[]{sdf.parse("2034-09-27"), sdf.parse("2034-09-29")});
            put(2035, new Date[]{sdf.parse("2035-09-16"), sdf.parse("2035-09-18")});
        }};

        /**
         * 元旦放假区间映射，key: 年份，value: [开始日期, 结束日期] 格式 yyyy-MM-dd
         * 放假3天
         */
       NEW_YEARS_DAY_RANGE_MAP = new HashMap<Integer, Date[]>() {{
            put(2025, new Date[]{sdf.parse("2025-01-01"), sdf.parse("2025-01-03")});
            put(2026, new Date[]{sdf.parse("2026-01-01"), sdf.parse("2026-01-03")});
            put(2027, new Date[]{sdf.parse("2027-01-01"), sdf.parse("2027-01-03")});
            put(2028, new Date[]{sdf.parse("2028-01-01"), sdf.parse("2028-01-03")});
            put(2029, new Date[]{sdf.parse("2029-01-01"), sdf.parse("2029-01-03")});
            put(2030, new Date[]{sdf.parse("2030-01-01"), sdf.parse("2030-01-03")});
            put(2031, new Date[]{sdf.parse("2031-01-01"), sdf.parse("2031-01-03")});
            put(2032, new Date[]{sdf.parse("2032-01-01"), sdf.parse("2032-01-03")});
            put(2033, new Date[]{sdf.parse("2033-01-01"), sdf.parse("2033-01-03")});
            put(2034, new Date[]{sdf.parse("2034-01-01"), sdf.parse("2034-01-03")});
            put(2035, new Date[]{sdf.parse("2035-01-01"), sdf.parse("2035-01-03")});
        }};

        /**
         * 劳动节放假区间映射，key: 年份，value: [开始日期, 结束日期] 格式 yyyy-MM-dd
         * 放假5天
         */
       LABOR_DAY_RANGE_MAP = new HashMap<Integer, Date[]>() {{
            put(2025, new Date[]{sdf.parse("2025-05-01"), sdf.parse("2025-05-05")});
            put(2026, new Date[]{sdf.parse("2026-05-01"), sdf.parse("2026-05-05")});
            put(2027, new Date[]{sdf.parse("2027-05-01"), sdf.parse("2027-05-05")});
            put(2028, new Date[]{sdf.parse("2028-05-01"), sdf.parse("2028-05-05")});
            put(2029, new Date[]{sdf.parse("2029-05-01"), sdf.parse("2029-05-05")});
            put(2030, new Date[]{sdf.parse("2030-05-01"), sdf.parse("2030-05-05")});
            put(2031, new Date[]{sdf.parse("2031-05-01"), sdf.parse("2031-05-05")});
            put(2032, new Date[]{sdf.parse("2032-05-01"), sdf.parse("2032-05-05")});
            put(2033, new Date[]{sdf.parse("2033-05-01"), sdf.parse("2033-05-05")});
            put(2034, new Date[]{sdf.parse("2034-05-01"), sdf.parse("2034-05-05")});
            put(2035, new Date[]{sdf.parse("2035-05-01"), sdf.parse("2035-05-05")});
        }};

        /**
         * 国庆节放假区间映射，key: 年份，value: [开始日期, 结束日期] 格式 yyyy-MM-dd
         * 放假7天
         */
       NATIONAL_DAY_RANGE_MAP = new HashMap<Integer, Date[]>() {{
            put(2025, new Date[]{sdf.parse("2025-10-01"), sdf.parse("2025-10-07")});
            put(2026, new Date[]{sdf.parse("2026-10-01"), sdf.parse("2026-10-07")});
            put(2027, new Date[]{sdf.parse("2027-10-01"), sdf.parse("2027-10-07")});
            put(2028, new Date[]{sdf.parse("2028-10-01"), sdf.parse("2028-10-07")});
            put(2029, new Date[]{sdf.parse("2029-10-01"), sdf.parse("2029-10-07")});
            put(2030, new Date[]{sdf.parse("2030-10-01"), sdf.parse("2030-10-07")});
            put(2031, new Date[]{sdf.parse("2031-10-01"), sdf.parse("2031-10-07")});
            put(2032, new Date[]{sdf.parse("2032-10-01"), sdf.parse("2032-10-07")});
            put(2033, new Date[]{sdf.parse("2033-10-01"), sdf.parse("2033-10-07")});
            put(2034, new Date[]{sdf.parse("2034-10-01"), sdf.parse("2034-10-07")});
            put(2035, new Date[]{sdf.parse("2035-10-01"), sdf.parse("2035-10-07")});
        }};
    }
 
    /**
     * 判断当前日期是否需要跳过调度任务执行
     * @return true-需要跳过，false-不需要跳过
     */
    public HolidayCheckResult isNeedSkip(){
       
        return isNeedSkip( new Date());
    }

    /**
     * 判断指定日期是否需要跳过调度任务执行
     * @param date
     * @return true-需要跳过，false-不需要跳过
     */
    public HolidayCheckResult isNeedSkip(  Date date){

        HolidayCheckResult holidayCheckResult = new HolidayCheckResult();
        // 判断星期几
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
        if(isSkipSaturday() && dayOfWeek == Calendar.SATURDAY){
            holidayCheckResult.setResult(true);
            holidayCheckResult.setMessage("Today is SATURDAY,ignore schedule.");
            return holidayCheckResult;
        }
        if(isSkipSunday() && dayOfWeek == Calendar.SUNDAY){
            holidayCheckResult.setResult(true);
            holidayCheckResult.setMessage("Today is SUNDAY,ignore schedule.");
            return holidayCheckResult;
        }

        // 判断各节日放假区间
        
        int year = calendar.get(Calendar.YEAR);

        if(isSkipNewYearsDay()){
            if(inRange(date, NEW_YEARS_DAY_RANGE_MAP.get(year))){
                holidayCheckResult.setResult(true);
                holidayCheckResult.setMessage("Today is in NEW_YEARS_DAY,ignore schedule.");
                return holidayCheckResult;
            }
        }
        if(isSkipLaborDay()){
            if(inRange(date, LABOR_DAY_RANGE_MAP.get(year))){
                holidayCheckResult.setResult(true);
                holidayCheckResult.setMessage("Today is in LABOR_DAY,ignore schedule.");
                return holidayCheckResult;
            }
        }
        if(isSkipNationalDay()){
            if(inRange(date, NATIONAL_DAY_RANGE_MAP.get(year))){
                holidayCheckResult.setResult(true);
                holidayCheckResult.setMessage("Today is in NATIONAL_DAY,ignore schedule.");
                return holidayCheckResult;
            }
        }
        if(isSkipSpringFestival()){
            if(inRange(date, SPRING_FESTIVAL_RANGE_MAP.get(year))){
                holidayCheckResult.setResult(true);
                holidayCheckResult.setMessage("Today is in SPRING_FESTIVAL,ignore schedule.");
                return holidayCheckResult;
            }
        }
        if(isSkipDragonBoatFestival()){
            if(inRange(date, DRAGON_BOAT_FESTIVAL_RANGE_MAP.get(year))){
                holidayCheckResult.setResult(true);
                holidayCheckResult.setMessage("Today is in DRAGON_BOAT_FESTIVAL,ignore schedule.");
                return holidayCheckResult;
            }
        }
        if(isSkipMidAutumnFestival()){
            if(inRange(date, MID_AUTUMN_FESTIVAL_RANGE_MAP.get(year))){
                holidayCheckResult.setResult(true);
                holidayCheckResult.setMessage("Today is in MID_AUTUMN_FESTIVAL,ignore schedule.");
                return holidayCheckResult;
            }
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = sdf.format(date);
        // 判断自定义节假日
        List<String> customHolidays = getCustomHolidays();
        if(customHolidays != null && !customHolidays.isEmpty()){
            for(String holiday : customHolidays){
                if(dateStr.equals(holiday)){
                    holidayCheckResult.setResult(true);
                    holidayCheckResult.setMessage("Today is "+holiday+",ignore schedule.");
                    return holidayCheckResult;
                }
            }
        }

        return holidayCheckResult;
    }

    /**
     * 判断日期是否在指定区间内
     * @param date 格式 yyyy-MM-dd
     * @param range [开始日期, 结束日期]
     * @return
     */
    private boolean inRange(Date date, Date[] range){
        if(range == null || range.length != 2){
            return false;
        }
        if(date.before(range[0]) || date.after(range[1])){
            return false;
        }
        return true;
    }


    /**
     * 添加自定义年份的春节放假区间
     * @param year
     * @param startDate 格式：yyyy-MM-dd
     * @param endDate 格式：yyyy-MM-dd
     */
    private HolidayScheduleConfig addDateRange(Map map,int year, String startDate, String endDate){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            map.put(year, new Date[]{sdf.parse(startDate), sdf.parse(endDate)});
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return this;
    }
    /**
     * 添加自定义年份的春节放假区间
     * @param year
     * @param startDate 格式：yyyy-MM-dd
     * @param endDate 格式：yyyy-MM-dd
     */
    public HolidayScheduleConfig addSpringFestivalRange(int year, String startDate, String endDate){
        return addDateRange(SPRING_FESTIVAL_RANGE_MAP,  year,   startDate,   endDate);
    } 

    /**
     * 添加自定义年份的端午节放假区间
     * @param year
     * @param startDate 格式：yyyy-MM-dd
     * @param endDate 格式：yyyy-MM-dd
     */
    public HolidayScheduleConfig addDragonBoatFestivalRange(int year, String startDate, String endDate){
        return addDateRange(DRAGON_BOAT_FESTIVAL_RANGE_MAP,  year,   startDate,   endDate);
    }

    /**
     * 添加自定义年份的中秋节放假区间
     * @param year
     * @param startDate 格式：yyyy-MM-dd
     * @param endDate 格式：yyyy-MM-dd
     */
    public HolidayScheduleConfig addMidAutumnFestivalRange(int year, String startDate, String endDate){
        return addDateRange(MID_AUTUMN_FESTIVAL_RANGE_MAP,  year,   startDate,   endDate);
    }

    /**
     * 添加自定义年份的元旦放假区间
     * @param year
     * @param startDate 格式：yyyy-MM-dd
     * @param endDate 格式：yyyy-MM-dd
     */
    public HolidayScheduleConfig addNewYearsDayRange(int year, String startDate, String endDate){
        return addDateRange(NEW_YEARS_DAY_RANGE_MAP,  year,   startDate,   endDate);
    }

    /**
     * 添加自定义年份的劳动节放假区间
     * @param year
     * @param startDate 格式：yyyy-MM-dd
     * @param endDate 格式：yyyy-MM-dd
     */
    public HolidayScheduleConfig addLaborDayRange(int year, String startDate, String endDate){
        return addDateRange(LABOR_DAY_RANGE_MAP,  year,   startDate,   endDate);
    }

    /**
     * 添加自定义年份的国庆节放假区间
     * @param year
     * @param startDate 格式：yyyy-MM-dd
     * @param endDate 格式：yyyy-MM-dd
     */
    public HolidayScheduleConfig addNationalDayRange(int year, String startDate, String endDate){
        return addDateRange(NATIONAL_DAY_RANGE_MAP,  year,   startDate,   endDate);
    }


}
