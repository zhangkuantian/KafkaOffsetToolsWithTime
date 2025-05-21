package com.lucas_hust.kafka.tools.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
// It's good practice to have a TimeConstants class if it defines the SimpleDateFormat objects.
// Assuming TimeConstants exists and is correctly defined in the same package or imported.

public class Utils {

    private static final String DEFAULT_DATE_LONG = "-1"; // Or throw exception for failed parse.
    private static final String DEFAULT_FORMATTED_DATE = "1970010100"; // Default for formatting methods.


    /**
     * Parses a string representation of a date into a long (milliseconds since epoch).
     *
     * @param strDate The string date to parse.
     * @param format  The format of the string date (e.g., "YYYY-MM-dd HH:mm:SS").
     * @return The date as milliseconds since epoch, or -1 if parsing fails.
     */
    public static long parseStringDate2Long(String strDate, String format) {
        if (strDate == null || format == null) {
            // Consider throwing IllegalArgumentException for null inputs for more robust error handling.
            System.err.println("Error: Date string or format string is null.");
            return Long.parseLong(DEFAULT_DATE_LONG);
        }

        SimpleDateFormat sdf;
        // Select the appropriate SimpleDateFormat instance based on the provided format string.
        switch (format) {
            case TimeConstants.YYYYMMDD_FORMAT:
                sdf = TimeConstants.YYYYMMDD;
                break;
            case TimeConstants.YYYYMMDD_HH_FORMAT:
                sdf = TimeConstants.YYYYMMDD_HH;
                break;
            case TimeConstants.YYYYMMDD_HHMM_FORMAT:
                sdf = TimeConstants.YYYYMMDD_HHMM;
                break;
            case TimeConstants.YYYYMMDD_HHMISS_FORMAT:
                sdf = TimeConstants.YYYYMMDD_HHMISS;
                break;
            case TimeConstants.YYYY_MM_DD_FORMAT:
                sdf = TimeConstants.YYYY_MM_DD;
                break;
            case TimeConstants.YYYY_MM_DD_HH_FORMAT:
                sdf = TimeConstants.YYYY_MM_DD_HH;
                break;
            case TimeConstants.YYYY_MM_DD_HH_MI_FORMAT:
                sdf = TimeConstants.YYYY_MM_DD_HH_MI;
                break;
            case TimeConstants.YYYY_MM_DD_HH_MI_SS_FORMAT:
                sdf = TimeConstants.YYYY_MM_DD_HH_MI_SS;
                break;
            default:
                // If the format string does not match any known format, print an error and return default.
                System.err.println("Error: Unknown date format string provided: " + format);
                return Long.parseLong(DEFAULT_DATE_LONG);
        }

        // Ensure a SimpleDateFormat object was successfully assigned.
        if (sdf != null) {
            try {
                // Parse the string date to a Date object.
                Date date = sdf.parse(strDate);
                // Return the time in milliseconds since the epoch.
                return date.getTime();
            } catch (ParseException e) {
                // If parsing fails, print a user-friendly error message and the stack trace.
                System.err.println("Error parsing date string: \"" + strDate + "\" with format: \"" + format + "\". Reason: " + e.getMessage());
                // e.printStackTrace(); // Optionally keep for debugging, or log if a logger is available.
            }
        } else {
             // This case should ideally not be reached if the switch handles all valid TimeConstant formats
             // and defaults for unknown formats. Added for defensive programming.
            System.err.println("Error: SimpleDateFormat object is null for format: " + format + ". This indicates an issue in date parsing logic.");
        }
        // Return default value if parsing failed or sdf was null.
        return Long.parseLong(DEFAULT_DATE_LONG);
    }

    /**
     * Gets the current date and time formatted as "yyyyMMddHH".
     *
     * @return The current date and time as a string.
     */
    public static String getCurrDateYYYYMMDDHH() {
        // No need to initialize 'ret' with a default if it's immediately overwritten.
        Date date = new Date();
        return TimeConstants.YYYYMMDDHH.format(date);
    }

    /**
     * Gets the current date and time formatted as "yyyyMMddHHmm".
     * Note: Original code used TimeConstants.YYYYMMDDHH, corrected to use a minute-precision formatter.
     * Assuming TimeConstants.YYYYMMDDHHMI exists or should be created for "yyyyMMddHHmm".
     *
     * @return The current date and time as a string.
     */
    public static String getCurrDateYYYYMMDDHHMI() {
        Date date = new Date();
        // Uses the dedicated formatter for "yyyyMMddHHmm".
        return TimeConstants.YYYYMMDDHHMI.format(date);
    }

}
