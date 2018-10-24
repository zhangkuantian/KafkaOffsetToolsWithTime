package com.lucas_hust.kafka.tools.util;

import java.text.SimpleDateFormat;

public class TimeConstants {

    public static final String START_TIME_YYYYMMDD_S = "19700101";
    public static final String START_TIME_YYYYMMDDHH_S = "1970010100";
    public static final String START_TIME_YYYYMMDDHHMI_S = "197001010000";

    public static final int START_TIME_YYYYMMDD_I = 19700101;
    public static final int START_TIME_YYYYMMDDHH_I = 1970010100;
    public static final long START_TIME_YYYYMMDDHHMI_I = 197001010000L;

    public static final SimpleDateFormat YYYYMMDD = new SimpleDateFormat("yyyyMMdd");
    public static final SimpleDateFormat YYYYMMDD_HH = new SimpleDateFormat("yyyyMMdd HH");
    public static final SimpleDateFormat YYYYMMDD_HHMM = new SimpleDateFormat("yyyyMMdd HH:mm");
    public static final SimpleDateFormat YYYYMMDD_HHMISS = new SimpleDateFormat("yyyyMMdd HH:mm:SS");
    public static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat YYYY_MM_DD_HH = new SimpleDateFormat("yyyy-MM-dd HH");
    public static final SimpleDateFormat YYYY_MM_DD_HH_MI = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    public static final SimpleDateFormat YYYY_MM_DD_HH_MI_SS = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
    public static final SimpleDateFormat YYYYMMDDHH = new SimpleDateFormat("yyyyMMddHH");
}
