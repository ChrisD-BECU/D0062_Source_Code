package com.corelationinc.utils;

import com.corelationinc.script.Money;
import com.corelationinc.script.ScriptException;
import com.corelationinc.script.Serial;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;

/**
 *
 * @author stosti
 */
public class ScriptShareUtils {

    /**
     * Determines if a share has been open for a given number of days.
     * <br></br>
     * ENV.POSTING_DATE - X DAYS >= SHARE.OPEN_DATE
     *
     * @param connection
     * @param shareSerial
     * @param days
     * @return
     * @throws SQLException
     * @throws com.corelationinc.script.ScriptException
     */
    public static boolean openForAtLeastXDays(Connection connection, Serial shareSerial, int days) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Null connection passed to openForAtLeastXDays.");
        }
        if (shareSerial == null) {
            throw new ScriptException("Null shareSerial passed to openForAtLeastXDays.");
        }
        if (days < 0) {
            throw new ScriptException("Negative number of days passed to openForAtLeastXDays.");
        }

        return getAgeInDays(connection, shareSerial) >= days;
    }


    /**
     * Returns the number of days a given share has been open.
     * <br></br>
     * DAYS(ENV.POSTING_DATE) - DAYS(SHARE.OPEN_DATE)
     *
     * @param connection
     * @param shareSerial
     * @return
     * @throws SQLException
     * @throws com.corelationinc.script.ScriptException
     */
    public static int getAgeInDays(Connection connection, Serial shareSerial) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Null connection passed to getAgeInDays.");
        }
        if (shareSerial == null) {
            throw new ScriptException("Null share serial passed to getAgeInDays.");
        }

        String sql = "SELECT"
                + "    DAYS(ENV.POSTING_DATE) - DAYS(SHARE.OPEN_DATE) AS DAYS_OPEN"
                + " FROM"
                + "    CORE.SHARE AS SHARE INNER JOIN"
                + "    CORE.ENV AS ENV ON"
                + "        ENV.SERIAL > 0"
                + " WHERE"
                + "    SHARE.SERIAL = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            shareSerial.set(stmt, 1);
            ResultSet rset = stmt.executeQuery();
            if (rset.next()) {
                return rset.getInt(1);
            }
            throw new ScriptException("No share found with given serial: " + shareSerial.toString());
        }
    }

    /**
     * Returns the number of days a given share has been open.
     * <br></br>
     * DAYS(ENV.POSTING_DATE) - DAYS(SHARE.OPEN_DATE)
     *
     * @param connection
     * @param shareStoredAccessKey
     * @return
     * @throws SQLException
     * @throws com.corelationinc.script.ScriptException
     */
    public static int getAgeInDays(Connection connection, String shareStoredAccessKey) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Null connection passed to getAgeInDays.");
        }
        if (shareStoredAccessKey == null) {
            throw new ScriptException("Null stored access key passed to getAgeInDays.");
        }

        String sql = "SELECT"
                + "    DAYS(ENV.POSTING_DATE) - DAYS(SHARE.OPEN_DATE) AS DAYS_OPEN"
                + " FROM"
                + "    CORE.SHARE AS SHARE INNER JOIN"
                + "    CORE.ENV AS ENV ON"
                + "        ENV.SERIAL > 0"
                + " WHERE"
                + "    SHARE.STORED_ACCESS_KEY = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, shareStoredAccessKey);
            ResultSet rset = stmt.executeQuery();
            if (rset.next()) {
                return rset.getInt(1);
            }
            throw new ScriptException("No share found with given stored access key: " + shareStoredAccessKey);
        }
    }

    /**
     * Determines if a share currently has a negative balance.
     *
     * @param connection
     * @param shareSerial
     * @return
     * @throws SQLException
     * @throws com.corelationinc.script.ScriptException
     */
    public static boolean isNegative(Connection connection, Serial shareSerial) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Null connection passed to isNegative.");
        }
        if (shareSerial == null) {
            throw new ScriptException("Null shareSerial passed to isNegative.");
        }

        String sql = " SELECT"
                + "    1"
                + " FROM"
                + "    CORE.SHARE AS SHARE "
                + " WHERE"
                + "    SHARE.SERIAL = ? AND"
                + "    SHARE.BALANCE < 0";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            shareSerial.set(stmt, 1);
            ResultSet rset = stmt.executeQuery();
            return rset.next();
        }
    }


    /**
     * Returns the number of consecutive days a share has carried a balance
     * below the given amount.
     *
     * @param connection
     * @param shareSerial
     * @param amount
     * @return
     * @throws SQLException
     * @throws com.corelationinc.script.ScriptException
     */
    public static int daysBelowAmount(Connection connection, Serial shareSerial, Money amount) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Null connection passed to daysBelowAmount.");
        }
        if (shareSerial == null) {
            throw new ScriptException("Null share serial passed to daysBelowAmount.");
        }
        if (amount == null) {
            throw new ScriptException("Null amount passed to daysBelowAmount.");
        }

        String sql = "WITH lastMonetaryAboveAmount(targetAccessKey,postingDate) AS ("
                + "    SELECT"
                + "        MONETARY.TARGET_ACCESS_KEY,"
                + "        MAX(MONETARY.POSTING_DATE)"
                + "    FROM"
                + "        CORE.MONETARY AS MONETARY "
                + "    WHERE"
                + "        MONETARY.STATUS='P' AND"
                + "        MONETARY.NEW_BALANCE < ? AND"
                + "        MONETARY.NEW_BALANCE - MONETARY.PRINCIPAL >= ?"
                + "    GROUP BY"
                + "        MONETARY.TARGET_ACCESS_KEY"
                + "    )"
                + " SELECT"
                + "    DAYS(ENV.POSTING_DATE) - DAYS(lastMonetaryAboveAmount.postingDate)"
                + " FROM"
                + "	CORE.SHARE AS SHARE INNER JOIN"
                + "	lastMonetaryAboveAmount ON"
                + "	   SHARE.STORED_ACCESS_KEY = lastMonetaryAboveAmount.targetAccessKey LEFT OUTER JOIN"
                + "    CORE.ENV AS ENV ON"
                + "        ENV.SERIAL > 0"
                + " WHERE"
                + "    SHARE.BALANCE < ? AND"
                + "    SHARE.SERIAL = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            amount.set(stmt, 1);
            amount.set(stmt, 2);
            amount.set(stmt, 3);
            shareSerial.set(stmt, 4);
            ResultSet rset = stmt.executeQuery();
            if (rset.next()) {
                return rset.getInt(1);
            }
            return 0;
        }
    }
}
