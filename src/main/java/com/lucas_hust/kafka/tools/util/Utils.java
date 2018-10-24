package com.lucas_hust.kafka.tools.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    public static long parseStringDate2Long(String strDate, String format){
        long ret = -1;
        SimpleDateFormat formats = null;
        switch (format){
            case "YYYYMMDD":
                formats = TimeConstants.YYYYMMDD;
                break;
            case "YYYYMMDD HH":
                formats = TimeConstants.YYYYMMDD_HH;
                break;
            case "YYYYMMDD HHmm":
                formats = TimeConstants.YYYYMMDD_HHMM;
                break;
            case "YYYYMMDD HHmmSS":
                formats = TimeConstants.YYYYMMDD_HHMISS;
                break;
            case "YYYY-MM-dd":
                formats = TimeConstants.YYYY_MM_DD;
                break;
            case "YYYY-MM-dd HH":
                formats = TimeConstants.YYYY_MM_DD_HH;
                break;
            case "YYYY-MM-dd HH:mm":
                formats = TimeConstants.YYYY_MM_DD_HH_MI;
                break;
            case "YYYY-MM-dd HH:mm:SS":
                formats = TimeConstants.YYYY_MM_DD_HH_MI_SS;
                break;
        }

        if(null != formats){
            try {
                Date date = formats.parse(strDate.replace("^"," "));
                ret = date.getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return ret;
    }

    public static String getCurrDateYYYYMMDDHH(){
        String ret = "1970010100";
        Date date = new Date();
        ret = TimeConstants.YYYYMMDDHH.format(date);
        return ret;
    }

    public static String getCurrDateYYYYMMDDHHMI(){
        String ret = "1970010100";
        Date date = new Date();
        ret = TimeConstants.YYYYMMDDHH.format(date);
        return ret;
    }

    public static void main(String []args){
        long ret = Utils.parseStringDate2Long("2018-10-22 12:30:33", "YYYY-MM-dd HH:mm:SS");
        System.out.println(ret);
    }
}
