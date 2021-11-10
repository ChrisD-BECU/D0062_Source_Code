package com.corelationinc.utils;

import com.corelationinc.script.ScriptException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

/**
 *
 * @author stosti
 */
public class ScriptDateUtils {

    /**
     * Returns the current posting date.
     *
     * @param connection
     * @return
     * @throws SQLException
     * @throws ScriptException
     */
    public static Date getToday(Connection connection) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Attempted to fetch current posting date from null connection.");
        } else if (connection.isClosed()) {
            throw new ScriptException("Attempted to fetch current posting date from closed connection.");
        }

        String sql = "SELECT"
                + "     ENV.POSTING_DATE"
                + " FROM"
                + "     CORE.ENV AS ENV";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            ResultSet rset = stmt.executeQuery();
            if (rset.next()) {
                return rset.getDate(1);
            } else {
                throw new ScriptException("Unable to fetch current posting date from database.");
            }
        }
    }

    /**
     * Returns the day before the current posting date.
     *
     * @param connection
     * @return
     * @throws SQLException
     * @throws ScriptException
     */
    public static Date getYesterday(Connection connection) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Attempted to fetch current posting date from null connection.");
        } else if (connection.isClosed()) {
            throw new ScriptException("Attempted to fetch current posting date from closed connection.");
        }

        String sql = "SELECT"
                + "     ENV.POSTING_DATE - 1 DAY"
                + " FROM"
                + "     CORE.ENV AS ENV";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            ResultSet rset = stmt.executeQuery();
            if (rset.next()) {
                return rset.getDate(1);
            } else {
                throw new ScriptException("Unable to fetch current posting date from database.");
            }
        }
    }

    /**
     * Returns the day after the current posting date.
     *
     * @param connection
     * @return
     * @throws SQLException
     * @throws ScriptException
     */
    public static Date getTomorrow(Connection connection) throws SQLException, ScriptException {
        Date today = getToday(connection);
        return addXDays(today, 1);
    }

    /**
     * Converts java.util.Date variables into java.sql.date variables.
     *
     * @param utilDate
     * @return
     */
    public static java.sql.Date getDate(java.util.Date utilDate) {
        if (utilDate == null) {
            return null;
        }
        long epochSeconds = utilDate.getTime();
        java.sql.Date sqlDate = new java.sql.Date(epochSeconds);
        return sqlDate;
    }

    /**
     * Returns a date variable corresponding to the date x days in the past.
     * <br></br>
     * <p>
     * ENV.POSTING_DATE - X DAYS</p>
     *
     * @param connection
     * @param days
     * @return
     * @throws SQLException
     * @throws com.corelationinc.script.ScriptException
     */
    public static Date getPreviousDate(Connection connection, int days) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Attempted to fetch previous posting date from null connection.");
        } else if (connection.isClosed()) {
            throw new ScriptException("Attempted to fetch previous posting date from closed connection.");
        }
        String sql = "SELECT"
                + "     ENV.POSTING_DATE - " + String.valueOf(days) + " DAYS"
                + " FROM"
                + "     CORE.ENV AS ENV";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            ResultSet rset = stmt.executeQuery();
            if (rset.next()) {
                return rset.getDate(1);
            } else {
                throw new ScriptException("Unable to fetch previous date from database.");
            }
        }
    }

    /**
     * Returns a date x number of years in the past, relative to the given date.
     * <br></br>
     * <p>
     * GIVEN_DATE - X YEARS</p>
     *
     * @param date
     * @param years
     * @return
     */
    public static Date subtractXYears(Date date, int years) {
        if (date == null) {
            return null;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.YEAR, -years);
        Date previousDate = new Date(cal.getTimeInMillis());
        return previousDate;
    }

    /**
     * Returns a date x number of years in the future, relative to the given
     * date.
     * <br></br>
     * <p>
     * GIVEN_DATE + X YEARS</p>
     *
     * @param date
     * @param years
     * @return
     */
    public static Date addXYears(Date date, int years) {
        if (date == null) {
            return null;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.YEAR, years);
        Date previousDate = new Date(cal.getTimeInMillis());
        return previousDate;
    }

    /**
     * Returns a date x number of months in the future, relative to the given
     * date.
     * <br></br>
     * <p>
     * GIVEN_DATE + X MONTHS</p>
     *
     * @param date
     * @param years
     * @return
     */
    public static Date addXMonths(Date date, int months) throws ScriptException {
        if (date == null) {
            return null;
        }
        if (months < 0) {
            throw new ScriptException("Negative month count passed to addXMonths.");
        }
        return manipulateCalendarXMonths(date, months);
    }

    /**
     * Returns a date x number of months in the past, relative to the given
     * date.
     * <br></br>
     * <p>
     * GIVEN_DATE - X MONTHS</p>
     *
     * @param date
     * @param years
     * @return
     */
    public static Date subtractXMonths(Date date, int months) throws ScriptException {
        if (date == null) {
            return null;
        }
        if (months < 0) {
            throw new ScriptException("Negative month count passed to subtractXMonths.");
        }
        return manipulateCalendarXMonths(date, -months);

    }

    private static Date manipulateCalendarXMonths(Date date, int months) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, months);
        Date newDate = new Date(cal.getTimeInMillis());
        return newDate;
    }

    /**
     * Returns a date x number of days in the past, relative to the given date.
     * <br></br>
     * <p>
     * GIVEN_DATE - X DAYS</p>
     *
     * @param date
     * @param days
     * @return
     */
    public static Date subtractXDays(Date date, int days) {
        return manipulateCalendarXDays(date, -days);
    }

    /**
     * Returns a date x number of days in the future, relative to the given
     * date.
     * <br></br>
     * <p>
     * GIVEN_DATE + X DAYS</p>
     *
     * @param date
     * @param days
     * @return
     */
    public static Date addXDays(Date date, int days) {
        return manipulateCalendarXDays(date, days);
    }

    private static Date manipulateCalendarXDays(Date date, int days) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, days);
        Date newDate = new Date(cal.getTimeInMillis());
        return days == 0 ? date : newDate;
    }

    /**
     * Returns the chronologically first date in a given list of dates.
     *
     * @param dates
     * @return
     */
    public static Date getEarliestDate(List<Date> dates) {
        if (dates == null || dates.isEmpty()) {
            return null;
        }
        Date earliest = dates.get(0);
        for (Date date : dates) {
            if (date.before(earliest)) {
                earliest = date;
            }
        }
        return earliest;
    }

    /**
     * Returns the chronologically last date in a given list of dates.
     *
     * @param dates
     * @return
     */
    public static Date getLatestDate(List<Date> dates) {
        if (dates == null || dates.isEmpty()) {
            return null;
        }
        Date latest = dates.get(0);
        for (Date date : dates) {
            if (date.after(latest)) {
                latest = date;
            }
        }
        return latest;
    }

    /**
     * Determines if a given date is between the provided start and end date.
     * (inclusive)
     * <br></br>
     * startDate must be chronologically before the endDate.
     *
     * @param startDate
     * @param endDate
     * @param targetDate
     * @return
     * @throws ScriptException
     */
    public static boolean dateBetween(Date startDate, Date endDate, Date targetDate) throws ScriptException {
        if (startDate == null) {
            throw new ScriptException("Cannot evaluate range for null date expression.");
        } else if (endDate == null) {
            throw new ScriptException("Cannot evaluate range for null date expression.");
        } else if (targetDate == null) {
            throw new ScriptException("Cannot evaluate range for null date expression.");
        }
        if (startDate.equals(targetDate)) {
            return true;
        }
        if (endDate.equals(targetDate)) {
            return true;
        }
        return targetDate.after(startDate) && targetDate.before(endDate);
    }

    /**
     * Determines the number of days between the provided start and end date.
     * (inclusive)
     * <br></br>
     * startDate must be chronologically before the endDate.
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ScriptException
     */
    public static int getDaysBetween(Date startDate, Date endDate) throws ScriptException {
        if (startDate == null) {
            throw new ScriptException("Cannot evaluate range for null date expression.");
        } else if (endDate == null) {
            throw new ScriptException("Cannot evaluate range for null date expression.");
        }

        if (startDate.equals(endDate)) {
            return 0;
        }

        LocalDate start = startDate.toLocalDate();
        LocalDate end = endDate.toLocalDate();
        int ctr = 1;
        if (start.isBefore(end)) {

            while (start.plusDays(ctr).isBefore(end)) {
                ctr++;
            }
            return ctr;

        } else {

            while (start.minusDays(ctr).isBefore(end)) {
                ctr++;
            }
            return -ctr;
        }

    }
    
    /**
     * Determines the number of months between the provided start and end date.
     * (inclusive)
     * <br></br>
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ScriptException
     */
    public static int getMonthsBetween(Date startDate, Date endDate) throws ScriptException {
        if (startDate == null) {
            throw new ScriptException("Cannot evaluate range for null date expression.");
        } else if (endDate == null) {
            throw new ScriptException("Cannot evaluate range for null date expression.");
        } 
        if (startDate.equals(endDate)) {
            return 0;
        }
        if (startDate.after(endDate)) {
            Date tempDate = startDate;
            startDate = endDate;
            endDate = tempDate;
        }
        int count = 0;
        Date tempDate = startDate;
        while (tempDate.before(endDate)) {
                count++;
                tempDate = ScriptDateUtils.addXMonths(startDate, count);
            }
        if (tempDate.equals(endDate)) {
            return count;
        } else {
            count--;
            return count;
        }
    }  
    
    /**
     * Converts a string in the pattern 'yyyy-MM-dd' into a java.sql.date
     * variable.
     *
     * @param dateString
     * @return
     * @throws ScriptException
     */
    public static Date getDateYYYYMMDD(String dateString) throws ScriptException {
        if (dateString == null || dateString.isEmpty()) {
            return null;
        }
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        java.util.Date utilDate;
        try {
            utilDate = df.parse(dateString);
        } catch (ParseException ex) {
            throw new ScriptException("Unable to parse string into date.");
        }
        return new Date(utilDate.getTime());
    }

    /*
     * Returns the first day of the month current month.
     * @param Connection
     * @return Date
     * @throws SQLException, ScriptException
     */
    public static Date getMonthStart(Connection connection) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Attempted to fetch current posting date from null connection.");
        } else if (connection.isClosed()) {
            throw new ScriptException("Attempted to fetch current posting date from closed connection.");
        }

        String sql = " SELECT "
                + " 	POSTING_DATE + 1 DAY - DAY(POSTING_DATE) DAYS"
                + " FROM"
                + " 	CORE.ENV";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            ResultSet rset = stmt.executeQuery();
            if (rset.next()) {
                return rset.getDate(1);
            } else {
                throw new ScriptException("Unable to fetch last day of the current month from database.");
            }
        }
    }

    /*
     * Returns the current posting date in letter format: "Month Day, Year".
     * @param Connection
     * @return String
     * @throws SQLException, ScriptException
     */
    public static String getTodayLetterDate(Connection connection) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Attempted to fetch current posting date from null connection.");
        } else if (connection.isClosed()) {
            throw new ScriptException("Attempted to fetch current posting date from closed connection.");
        }

        String sql = " SELECT "
                + " 	MONTHNAME(POSTING_DATE) || ' ' || DAY(POSTING_DATE) || ', ' || YEAR(POSTING_DATE) AS GOOD_DATES"
                + " FROM"
                + " 	CORE.ENV";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            ResultSet rset = stmt.executeQuery();
            if (rset.next()) {
                return rset.getString(1);
            } else {
                throw new ScriptException("Unable to fetch curent day from database.");
            }
        }
    }

    /*
     * Returns SQL date from string using SimpleDateFormat string.
     * @param datestring
     * @param dateFormat 
     * @return Date
     * @throws ScriptException
     */
    public static Date getDateFromString(String dateString, String dateFormat) throws ScriptException {
        SimpleDateFormat df = new SimpleDateFormat(dateFormat);
        try {
            java.util.Date utilDay = (java.util.Date) df.parse(dateString);
            Date date = new java.sql.Date(utilDay.getTime());
            return date;
        } catch (Exception e) {
            throw new ScriptException("Invalid date format.");
        }
    }


    /*
     * Returns first day of the previous month.
     * @param Connection
     * @return Date
     * @throws SQLException, ScriptException
     */
    public static Date getMonthStartPreviousMonth(Connection connection) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Attempted to fetch current posting date from null connection.");
        } else if (connection.isClosed()) {
            throw new ScriptException("Attempted to fetch current posting date from closed connection.");
        }

        String sql = " SELECT "
                + "	POSTING_DATE + 1 DAY - DAY(POSTING_DATE) DAYS - DAY(POSTING_DATE + 1 DAY - DAY(POSTING_DATE) DAYS - 1 DAY) DAYS "
                + " FROM"
                + "	CORE.ENV AS ENV";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            ResultSet rset = stmt.executeQuery();
            if (rset.next()) {
                return rset.getDate(1);
            } else {
                throw new ScriptException("Unable to fetch ENV from database.");
            }
        }
    }

    /*
     * Returns last day of the previous month
     * @param Connection
     * @return Date
     * @throws SQLException, ScriptException
     */
    public static String getYear(Date targetDate) throws Exception {
        return getValuesFromDate(targetDate, 0, 4);
    }

    public static String getMonth(Date targetDate) throws Exception {
        return getValuesFromDate(targetDate, 5, 7);
    }

    public static String getDays(Date targetDate) throws Exception {
        return getValuesFromDate(targetDate, 8, 10);
    }

    private static String getValuesFromDate(Date targetDate, int beginIndex, int endIndex) throws Exception {
        if (targetDate == null) {
            throw new ScriptException("Cannot pass a null target date");
        }

        String date = targetDate.toString();

        if (date.length() != 10) {
            throw new ScriptException("Invalid date size, it must be 10 characters long");
        }

        return date.substring(beginIndex, endIndex);
    }

    public static Date getMonthEndPreviousMonth(Connection connection) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Attempted to fetch current posting date from null connection.");
        } else if (connection.isClosed()) {
            throw new ScriptException("Attempted to fetch current posting date from closed connection.");
        }

        String sql = " SELECT "
                + " 	LAST_DAY(POSTING_DATE - 1 MONTH)"
                + " FROM"
                + " 	CORE.ENV";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            ResultSet rset = stmt.executeQuery();
            if (rset.next()) {
                return rset.getDate(1);
            } else {
                throw new ScriptException("Unable to fetch ENV from database.");
            }
        }
    }

    public enum CalandarQuarter {
        FIRST,
        SECOND,
        THIRD,
        FOURTH
    }

    public enum CalandarMonth {
        JANUARY("January", 1, 31),
        FEBRUARY("February", 2, 28),
        MARCH("March", 3, 31),
        APRIL("April", 4, 30),
        MAY("May", 5, 31),
        JUNE("June", 6, 30),
        JULY("July", 7, 31),
        AUGUST("August", 8, 31),
        SEPTEMBER("september", 9, 30),
        OCTOBER("October", 10, 31),
        NOVEMBER("November", 11, 30),
        DECEMBER("December", 12, 31);

        private final String calandarName;
        private final int calandarIndex;
        private final int daysInMonth;
        private final int qtr;

        CalandarMonth(String name, int index, int daysInMonth) {
            this.calandarName = name;
            this.calandarIndex = index;
            this.daysInMonth = daysInMonth;
            if (this.calandarIndex < 4) {
                this.qtr = 1;
            } else if (this.calandarIndex < 7) {
                this.qtr = 2;
            } else if (this.calandarIndex < 10) {
                this.qtr = 3;
            } else {
                this.qtr = 4;
            }
        }

        public String getCalandarName() {
            return this.calandarName;
        }

        public int getCalandarIndex() {
            return this.calandarIndex;
        }

        public String getCalandarIndexAsString() {
            return this.calandarIndex > 9
                    ? String.valueOf(this.calandarIndex)
                    : "0" + String.valueOf(this.calandarIndex);
        }

        public int getDaysInCalandarMonth() {
            return this.daysInMonth;
        }

        public String getDaysInCalandarMonthAsString() {
            return String.valueOf(this.daysInMonth);
        }

        public int getCalandarMonthQuarter() {
            return this.qtr;
        }

        public static CalandarMonth getMonth(int month) {
            switch (month) {
                case 1:
                    return CalandarMonth.JANUARY;
                case 2:
                    return CalandarMonth.FEBRUARY;
                case 3:
                    return CalandarMonth.MARCH;
                case 4:
                    return CalandarMonth.APRIL;
                case 5:
                    return CalandarMonth.MAY;
                case 6:
                    return CalandarMonth.JUNE;
                case 7:
                    return CalandarMonth.JULY;
                case 8:
                    return CalandarMonth.AUGUST;
                case 9:
                    return CalandarMonth.SEPTEMBER;
                case 10:
                    return CalandarMonth.OCTOBER;
                case 11:
                    return CalandarMonth.NOVEMBER;
                default:
                    return CalandarMonth.DECEMBER;

            }
        }

        public static List<CalandarMonth> getCalandarYear() {
            return Arrays.asList(CalandarMonth.JANUARY,
                    CalandarMonth.FEBRUARY,
                    CalandarMonth.MARCH,
                    CalandarMonth.APRIL,
                    CalandarMonth.MAY,
                    CalandarMonth.JUNE,
                    CalandarMonth.JULY,
                    CalandarMonth.AUGUST,
                    CalandarMonth.SEPTEMBER,
                    CalandarMonth.OCTOBER,
                    CalandarMonth.NOVEMBER,
                    CalandarMonth.DECEMBER);
        }
    }

    public static class DateSet {

        private final Date startDate;
        private final Date endDate;

        public DateSet(Date start, Date end) {
            this.startDate = start;
            this.endDate = end;
        }

        public Date getStartDate() {
            return this.startDate;
        }

        public Date getEndDate() {
            return this.endDate;
        }
    }

    public static DateSet getMonthStartEndDates(CalandarMonth month, String year) throws Exception {
        if (year == null || year.isEmpty()) {
            throw new ScriptException("Cannot pass a null or empty year");
        }

        if (year.length() != 4) {
            throw new ScriptException("The year must be 4 characters long");
        }

        String tempDate = year + "-" + month.getCalandarIndexAsString() + "-";
        Date startDate = Date.valueOf(tempDate + "01");
        Date endDate;
        if (month.equals(CalandarMonth.FEBRUARY) && isLeapYear(year)) {
            endDate = Date.valueOf(tempDate + "29");
        } else {
            endDate = Date.valueOf(tempDate + month.getDaysInCalandarMonthAsString());
        }

        return new DateSet(startDate, endDate);
    }

    public static boolean isLeapYear(Date date) throws Exception {
        return isLeapYear(getYear(date));
    }

    public static boolean isLeapYear(String year) throws Exception {
        if (year == null || year.isEmpty()) {
            throw new ScriptException("Cannot pass a null or empty year");
        }

        if (year.length() != 4) {
            throw new ScriptException("The year must be 4 characters long");
        }

        Integer leapYear = Integer.valueOf(year);

        if (leapYear % 400 == 0) {
            return true;
        }

        return leapYear % 100 != 0 && leapYear % 4 == 0;
    }
}
