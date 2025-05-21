package com.lucas_hust.kafka.tools.util;

import java.text.SimpleDateFormat;

public class TimeConstants {

    // Start time constants (less relevant now, but kept for completeness if used elsewhere)
    public static final String START_TIME_YYYYMMDD_S = "19700101";
    public static final String START_TIME_YYYYMMDDHH_S = "1970010100";
    public static final String START_TIME_YYYYMMDDHHMI_S = "197001010000";

    public static final int START_TIME_YYYYMMDD_I = 19700101;
    public static final int START_TIME_YYYYMMDDHH_I = 1970010100;
    public static final long START_TIME_YYYYMMDDHHMI_I = 197001010000L;

    // Date Format Strings
    public static final String YYYYMMDD_FORMAT = "yyyyMMdd";
    public static final String YYYYMMDD_HH_FORMAT = "yyyyMMdd HH";
    public static final String YYYYMMDD_HHMM_FORMAT = "yyyyMMdd HH:mm";
    public static final String YYYYMMDD_HHMISS_FORMAT = "yyyyMMdd HH:mm:SS";
    public static final String YYYY_MM_DD_FORMAT = "yyyy-MM-dd";
    public static final String YYYY_MM_DD_HH_FORMAT = "yyyy-MM-dd HH";
    public static final String YYYY_MM_DD_HH_MI_FORMAT = "yyyy-MM-dd HH:mm";
    public static final String YYYY_MM_DD_HH_MI_SS_FORMAT = "yyyy-MM-dd HH:mm:SS";
    public static final String YYYYMMDDHH_FORMAT = "yyyyMMddHH";
    public static final String YYYYMMDDHHMI_FORMAT = "yyyyMMddHHmm"; // For getCurrDateYYYYMMDDHHMI

    // SimpleDateFormat objects initialized with the format strings
    public static final SimpleDateFormat YYYYMMDD = new SimpleDateFormat(YYYYMMDD_FORMAT);
    public static final SimpleDateFormat YYYYMMDD_HH = new SimpleDateFormat(YYYYMMDD_HH_FORMAT);
    public static final SimpleDateFormat YYYYMMDD_HHMM = new SimpleDateFormat(YYYYMMDD_HHMM_FORMAT);
    public static final SimpleDateFormat YYYYMMDD_HHMISS = new SimpleDateFormat(YYYYMMDD_HHMISS_FORMAT);
    public static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat(YYYY_MM_DD_FORMAT);
    public static final SimpleDateFormat YYYY_MM_DD_HH = new SimpleDateFormat(YYYY_MM_DD_HH_FORMAT);
    public static final SimpleDateFormat YYYY_MM_DD_HH_MI = new SimpleDateFormat(YYYY_MM_DD_HH_MI_FORMAT);
    public static final SimpleDateFormat YYYY_MM_DD_HH_MI_SS = new SimpleDateFormat(YYYY_MM_DD_HH_MI_SS_FORMAT);
    public static final SimpleDateFormat YYYYMMDDHH = new SimpleDateFormat(YYYYMMDDHH_FORMAT);
    public static final SimpleDateFormat YYYYMMDDHHMI = new SimpleDateFormat(YYYYMMDDHHMI_FORMAT); // For getCurrDateYYYYMMDDHHMI
}
