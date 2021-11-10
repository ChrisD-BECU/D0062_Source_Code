package com.corelationinc.utils;

import com.corelationinc.script.Serial;
import com.corelationinc.script.Money;
import com.corelationinc.script.ScriptException;
import com.corelationinc.utils.ScriptUtils.LOAN_TYPE_CATEGORY;
import com.corelationinc.utils.ScriptMathUtils;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author stosti
 */
public class ScriptAccountUtils {

    /**
     * Determines there are any charged-off shares / loans associated with the
     * given account.
     *
     * @param connection
     * @param accountSerial
     * @return true if charged-off share or loan exists beneath account
     * @throws SQLException
     * @throws com.corelationinc.script.ScriptException
     */
    public static boolean hasChargeOffs(Connection connection, Serial accountSerial) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Null connection passed to hasChargeOffs.");
        }
        if (accountSerial == null) {
            throw new ScriptException("Null account serial passed to hasChargeOffs.");
        }

        boolean hasChargeOffShares = hasChargedOffShares(connection, accountSerial);
        boolean hasChargeOffLoans = hasChargedOffLoans(connection, accountSerial);
        return hasChargeOffShares || hasChargeOffLoans;
    }

    /**
     * Determines there are any charged-off shares associated with the given
     * account.
     *
     * @param connection
     * @param accountSerial
     * @return true if charged-off share exists beneath account
     * @throws SQLException
     * @throws com.corelationinc.script.ScriptException
     */
    public static boolean hasChargedOffShares(Connection connection, Serial accountSerial) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Null connection passed to hasChargedOffShares.");
        }
        if (accountSerial == null) {
            throw new ScriptException("Null account serial passed to hasChargedOffShares.");
        }

        String sql = "SELECT"
                + "    1"
                + " FROM"
                + "    CORE.SHARE"
                + " WHERE"
                + "    CHARGE_OFF_DATE IS NOT NULL AND"
                + "    PARENT_SERIAL = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            accountSerial.set(stmt, 1);
            ResultSet rset = stmt.executeQuery();
            return rset.next();
        }
    }

    /**
     * Determines there are any charged-off loans associated with the given
     * account.
     *
     * @param connection
     * @param accountSerial
     * @return true if charged-off loan exists beneath account
     * @throws SQLException
     * @throws com.corelationinc.script.ScriptException
     */
    public static boolean hasChargedOffLoans(Connection connection, Serial accountSerial) throws SQLException, ScriptException {
        if (connection == null) {
            throw new ScriptException("Null connection passed to hasChargedOffLoans.");
        }
        if (accountSerial == null) {
            throw new ScriptException("Null account serial passed to hasChargedOffLoans.");
        }

        String sql = "SELECT"
                + "    1"
                + " FROM"
                + "    CORE.LOAN"
                + " WHERE"
                + "    CHARGE_OFF_DATE IS NOT NULL AND"
                + "    PARENT_SERIAL = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            accountSerial.set(stmt, 1);
            ResultSet rset = stmt.executeQuery();
            return rset.next();

        }
    }
    /**
     * Determines if the given account has any loans which are delinquent
     * more-than or equal-to the given number of days.
     *
     * @param connection
     * @param accountSerial
     * @param minimumDaysDelinquent
     * @return
     * @throws SQLException
     * @throws com.corelationinc.script.ScriptException
     */
    public static boolean hasDelinquentLoan(Connection connection, Serial accountSerial, int minimumDaysDelinquent) throws SQLException, ScriptException {
        return hasDelinquentLoan(
                connection,
                accountSerial,
                minimumDaysDelinquent,
                LOAN_TYPE_CATEGORY.CLOSED_END,
                LOAN_TYPE_CATEGORY.CREDIT_CARD,
                LOAN_TYPE_CATEGORY.LINE_OF_CREDIT,
                LOAN_TYPE_CATEGORY.OPEN_END);
    }

    /**
     *
     * @param connection
     * @param accountSerial
     * @param minimumDaysDelinquent
     * @param categories
     * @return
     * @throws ScriptException
     * @throws SQLException
     */
    public static boolean hasDelinquentLoan(Connection connection, Serial accountSerial, int minimumDaysDelinquent, LOAN_TYPE_CATEGORY... categories) throws ScriptException, SQLException {
        if (connection == null) {
            throw new ScriptException("Null connection passed to hasDelinquentLoan.");
        }
        if (accountSerial == null) {
            throw new ScriptException("Null account serial passed to hasDelinquentLoan.");
        }
        if (minimumDaysDelinquent < 0) {
            throw new ScriptException("Negative minimum days delinquent passed to hasDelinquentLoan.");
        }
        if (categories == null) {
            throw new ScriptException("Empty loan category list passed to hasDelinquentLoan.");
        }

        Set<String> loanTypeCategories = new HashSet<>();
        for (LOAN_TYPE_CATEGORY category : categories) {
            switch (category) {
                case CLOSED_END:
                    loanTypeCategories.add("CE");
                    break;
                case OPEN_END:
                    loanTypeCategories.add("OE");
                    break;
                case LINE_OF_CREDIT:
                    loanTypeCategories.add("LC");
                    break;
                case CREDIT_CARD:
                    loanTypeCategories.add("CC");
                    break;
            }
        }
        String sql = "SELECT"
                + "    1"
                + " FROM"
                + "    CORE.LOAN AS LOAN INNER JOIN"
                + "    CORE.LN_TYPE AS LN_TYPE ON"
                + "        LOAN.TYPE_SERIAL = LN_TYPE.SERIAL LEFT OUTER JOIN"
                + "    CORE.ENV AS ENV ON"
                + "        ENV.SERIAL > 0"
                + " WHERE"
                + "    LOAN.PARENT_SERIAL = ? AND"
                + "    LOAN.PAYMENT_DUE_DATE + ? DAYS < ENV.POSTING_DATE AND"
                + "    LOAN.BALANCE <> 0 AND"
                + "    LN_TYPE.CATEGORY IN (" + ScriptUtils.createInStatementVariables(loanTypeCategories) + ") AND"
                + "    LOAN.CLOSE_DATE IS NULL AND"
                + "    LOAN.CHARGE_OFF_DATE IS NULL";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            int i = 1;
            accountSerial.set(stmt, i++);
            stmt.setInt(i++, minimumDaysDelinquent);
            for (String category : loanTypeCategories) {
                stmt.setString(i++, category);
            }
            ResultSet rset = stmt.executeQuery();
            return rset.next();
        }
    }
}
