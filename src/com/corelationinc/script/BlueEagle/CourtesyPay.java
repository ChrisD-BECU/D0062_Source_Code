package com.corelationinc.script.BlueEagle;

import com.corelationinc.script.*;
import com.corelationinc.utils.ScriptAccountUtils;
import com.corelationinc.utils.ScriptPersonUtils;
import com.corelationinc.utils.ScriptShareUtils;
import com.corelationinc.utils.MultiThreadScript;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author kgillooley
 * @title Courtesy Pay
 * @ticket BEC-59
 */
public class CourtesyPay extends MultiThreadScript {

	Serial negativeBalanceRestriction = null;
	Serial dqLoansRestriction = null;
	Serial chargeOffRestriction = null;
	Serial secondChanceCPRestriction = null;

	@Override
	public void beforeTasks() throws SQLException, ScriptException {
		negativeBalanceRestriction = getRestrictionSerial(getConnection(), "Negative Balance Days > 30");
		dqLoansRestriction = getRestrictionSerial(getConnection(), "Delinquent Loans");
		chargeOffRestriction = getRestrictionSerial(getConnection(), "Charged Off Loans/Accounts");
		secondChanceCPRestriction = getRestrictionSerial(getConnection(), "Second Chance");
		setNextTaskParameters(secondChanceCPRestriction);
	}

	public static void runScript(Script script) {
		MultiThreadScript manager = new CourtesyPay(script);
		manager.run();
	}

	public CourtesyPay(Script value) {
		super(value);
	}

	@Override
	protected String getNextTaskSQL() throws ScriptException {
		return "SELECT "
			+ "	PERSON.SERIAL,"
			+ "	ACCOUNT.ACCOUNT_NUMBER || ' S ' || SHARE.ID AS ACCOUNT_NUMBER,"
			+ "	ACCOUNT.SERIAL, "
			+ "	SHARE.SERIAL,"
			+ "	SHARE.COURTESY_PAY_RESTRICT_SERIAL,"
			+ "	SHARE.COURTESY_PAY_LIMIT"
			+ " FROM "
			+ "	CORE.PERSON AS PERSON INNER JOIN "
			+ "	CORE.ACCOUNT AS ACCOUNT ON "
			+ "		PERSON.SERIAL = ACCOUNT.PRIMARY_PERSON_SERIAL INNER JOIN "
			+ "	CORE.SHARE AS SHARE ON "
			+ "		ACCOUNT.SERIAL = SHARE.PARENT_SERIAL INNER JOIN "
			+ "	CORE.SH_TYPE AS SH_TYPE ON "
			+ "		SHARE.TYPE_SERIAL = SH_TYPE.SERIAL "
			+ " WHERE "
			+ "	SHARE.CLOSE_DATE IS NULL AND "
			+ "	SHARE.CHARGE_OFF_DATE IS NULL AND "
			+ "	SH_TYPE.DESCRIPTION = 'Checking - Regular' AND "
			+ "	(SHARE.COURTESY_PAY_RESTRICT_SERIAL IS NULL OR SHARE.COURTESY_PAY_RESTRICT_SERIAL <> ?)";
	}

	@Override
	protected MultiThreadTask getTask() throws ScriptException {
		return new MultiThreadTaskTemplate(this);
	}

	private class MultiThreadTaskTemplate extends MultiThreadTask {

		Serial personSerial = null;
		String accountNumber = null;
		Serial accountSerial = null;
		Serial shareSerial = null;
		Serial cpRestriction = null;
		Money cpLimit = null;
		Serial newCPRestriction = null;
		Money newCPLimit = null;

		public MultiThreadTaskTemplate(MultiThreadScript manager) {
			super(manager);
		}

		@Override
		public void perform() throws Exception {
			personSerial = getSerial();
			accountNumber = getString();
			accountSerial = getSerial();
			shareSerial = getSerial();
			cpRestriction = getSerial();
			cpLimit = getMoney();

			boolean hasCPRestriction = hasCPRestriction(getConnection(), shareSerial);
			if (isOldestShare(getConnection(), personSerial, shareSerial)) {
				enableOutput();
				boolean hasDelinquentLoans = ScriptAccountUtils.hasDelinquentLoan(getConnection(), accountSerial, 29);
				//add or restore CP
				if (cpLimit.isZero()) {
					boolean openDays = ScriptShareUtils.openForAtLeastXDays(getConnection(), shareSerial, 90);
					double aggregateDeposits = aggregateDeposits(getConnection(), shareSerial);
					boolean isOfAge = ScriptPersonUtils.isAtLeastAge(getConnection(), personSerial, 18);
					boolean hasChargeOffLoans = ScriptAccountUtils.hasChargedOffLoans(getConnection(), accountSerial);
					if (!hasCPRestriction && ScriptShareUtils.openForAtLeastXDays(getConnection(), shareSerial, 90) && (aggregateDeposits >= 1500.00) && isOfAge && !hasDelinquentLoans && !hasChargeOffLoans) {
						newCPRestriction = null;
						newCPLimit = new Money(50000);
					} else if (cpRestriction.equals(negativeBalanceRestriction) && !ScriptShareUtils.isNegative(getConnection(), shareSerial)) {
						newCPRestriction = null;
						newCPLimit = new Money(50000);
					} else if (cpRestriction.equals(dqLoansRestriction) && !hasDelinquentLoans) {
						newCPRestriction = null;
						newCPLimit = new Money(50000);
					} else {
						disableOutput();
					}
				}
				//remove CP
				else {
					if (ScriptShareUtils.daysBelowAmount(getConnection(), shareSerial, new Money(0)) >= 30) {
						newCPRestriction = negativeBalanceRestriction;
						newCPLimit = new Money(0);
					} else if (hasDelinquentLoans) {
						newCPRestriction = dqLoansRestriction;
						newCPLimit = new Money(0);
					} else if (ScriptAccountUtils.hasChargeOffs(getConnection(), accountSerial)) {
						newCPRestriction = chargeOffRestriction;
						newCPLimit = new Money(0);
					} else {
						disableOutput();
					}
				}
			} else {
				disableOutput();
			}
		}

		@Override
		public void output() throws Exception {
			XMLSerialize xml = getXMLReport("Courtesy Pay");
			if (!this.cpRestriction.equals(this.newCPRestriction) || !this.cpLimit.equals(newCPLimit)) {
				xml.putSequence();
				xml.putTransaction();
				xml.put("exceptionDescriptionPrefix", this.accountNumber);
				xml.putStep();
				xml.putRecord();
				{
					xml.putOption("operation", "U");
					xml.put("tableName", "SHARE");
					xml.put("targetSerial", this.shareSerial);
					xml.put("field");
					{
						xml.put("columnName", "COURTESY_PAY_LIMIT");
						xml.putForce("oldContents", this.cpLimit.toKeyBridgeString());
						xml.putForce("newContents", this.newCPLimit.toKeyBridgeString());
					}
					xml.put(); // </field>

					xml.put("field");
					{
						xml.put("columnName", "COURTESY_PAY_RESTRICT_SERIAL");
						xml.putForce("oldContents", this.cpRestriction.toKeyBridgeString());
						if (this.newCPRestriction == null) {
							xml.putForce("newContents", null);
						} else {
							xml.putForce("newContents", this.newCPRestriction.toKeyBridgeString());
						}
					}
					xml.put(); // </field>
				}
				xml.put(); // </record>
				xml.put(); // </step>
				xml.put(); // </transaction>
				xml.put(); // </sequence>
			}
		}

		private double aggregateDeposits(Connection connection, Serial shareSerial) throws SQLException {
			int count = 0;
			double aggregate = 0;
			String sql = "SELECT "
				+ "	COUNT(MONETARY_SERIAL),"
				+ "	SUM(PRINCIPAL)"
				+ " FROM"
				+ "("
				+ "	SELECT "
				+ "		MONETARY.SERIAL AS MONETARY_SERIAL, "
				+ "		MONETARY.PRINCIPAL "
				+ "	FROM "
				+ "		CORE.MONETARY AS MONETARY INNER JOIN "
				+ "		CORE.SHARE AS SHARE ON"
				+ "			MONETARY.TARGET_ACCESS_KEY = SHARE.STORED_ACCESS_KEY AND "
				+ "			MONETARY.STATUS = 'P' AND "
				+ "			MONETARY.CATEGORY = 'D' AND "
				+ "			MONETARY.TRANSFER_OPTION <> 'T' INNER JOIN "
				+ "		CORE.ENV AS ENV ON " + " ENV.SERIAL > 0"
				+ "	WHERE "
				+ "		MONETARY.POSTING_DATE > ENV.POSTING_DATE - 90 DAYS AND "
				+ "     LCASE(MONETARY.DESCRIPTION) NOT LIKE '%loan proceeds%' AND " //Excluding Loan Proceeds
				+ "     NOT EXISTS (" // <-----ADDED STATEMENT TO FILTER OUT ACH IRS PAYMENTS FROM BEING COUNTED----------->
				+ "     		SELECT 1 "
				+ "        		FROM CORE.MON_DETAIL MON_DETAIL"
				+ "             WHERE "
				+ "                 MON_DETAIL.PARENT_SERIAL = MONETARY.SERIAL AND "
				+ "                 MONETARY.SOURCE = 'a' AND "
				+ "             	MON_DETAIL.CATEGORY = 'ACH' AND "
				+ "                 (MON_DETAIL.CONTENTS_1 LIKE '%IRS%' OR MON_DETAIL.CONTENTS_1 LIKE '%VEC%')"//<--Added VEC payments to the exclusions -->
				+ "         ) AND " //END ADDED STATEMENT
				+ "		SHARE.SERIAL = ?"
				+ " )";
			PreparedStatement stmt = connection.prepareStatement(sql);
			int i = 1;
			shareSerial.set(stmt, i++);
			ResultSet rset = stmt.executeQuery();
			if (rset.next()) {
				count = rset.getInt(1);
				aggregate = rset.getDouble(2);
			}
			stmt.close();
			if (count >= 3) {
				return aggregate;
			} else {
				return 0;
			}
		}

		private boolean isOldestShare(Connection connection, Serial personSerial, Serial shareSerial) throws SQLException {
			boolean isOldestShare = false;
			Serial oldestShare = null;
			String sql = "SELECT "
				+ "	SHARE_SERIAL "
				+ " FROM"
				+ " ("
				+ "	SELECT "
				+ "		SHARE.SERIAL AS SHARE_SERIAL,"
				+ "		SHARE.OPEN_DATE,"
				+ "		MIN(SHARE.OPEN_DATE) OVER (PARTITION BY PERSON.SERIAL) AS MIN_OPEN_DATE"
				+ "	FROM "
				+ "		CORE.PERSON AS PERSON INNER JOIN "
				+ "         CORE.ACCOUNT AS ACCOUNT ON "
				+ "             PERSON.SERIAL = ACCOUNT.PRIMARY_PERSON_SERIAL INNER JOIN "
				+ " 	CORE.SHARE AS SHARE ON "
				+ "             ACCOUNT.SERIAL = SHARE.PARENT_SERIAL INNER JOIN"
				+ "         CORE.SH_TYPE AS SHARE_TYPE ON "
				+ "             SHARE.TYPE_SERIAL = SHARE_TYPE.SERIAL"
				+ " WHERE"
				+ "     SHARE.CLOSE_DATE IS NULL AND "
				+ "     SHARE.CHARGE_OFF_DATE IS NULL AND "
				+ "     SHARE_TYPE.DESCRIPTION = 'Checking - Regular' AND"
				+ "		ACCOUNT.CLOSE_DATE IS NULL AND "
				+ "		SHARE.CLOSE_DATE IS NULL AND "
				+ "		ACCOUNT.PRIMARY_PERSON_SERIAL = ?"
				+ " )"
				+ " WHERE"
				+ "	OPEN_DATE = MIN_OPEN_DATE "
				+ " ORDER BY "
				+ "     SHARE_SERIAL";
			PreparedStatement stmt = connection.prepareStatement(sql);
			personSerial.set(stmt, 1);
			ResultSet rset = stmt.executeQuery();
			if (rset.next()) {
				oldestShare = Serial.get(rset, 1);
				isOldestShare = shareSerial.equals(oldestShare);
			}
			stmt.close();
			return isOldestShare;
		}

	}

	private Serial getRestrictionSerial(Connection connection, String cpRestrictionDescription) throws SQLException, ScriptException {
		String sql = "SELECT "
			+ "	SERIAL "
			+ " FROM "
			+ "	CORE.COURTESY_PAY_RESTRICTION "
			+ " WHERE "
			+ "	DESCRIPTION = ?";
		Serial restrictionSerial = null;
		PreparedStatement stmt = connection.prepareStatement(sql);
		stmt.setString(1, cpRestrictionDescription);
		ResultSet rset = stmt.executeQuery();
		if (rset.next()) {
			restrictionSerial = Serial.get(rset, 1);
		} else {
			throw new ScriptException("Could not identify Courtesy Pay Restriction: " + cpRestrictionDescription);
		}
		stmt.close();
		return restrictionSerial;
	}

	private boolean hasCPRestriction(Connection connection, Serial shareSerial) throws SQLException {
		String sql = "SELECT "
			+ "	1"
			+ " FROM "
			+ "	CORE.SHARE AS SHARE INNER JOIN "
			+ "	CORE.COURTESY_PAY_RESTRICTION AS COURTESY_PAY_RESTRICTION ON "
			+ "		SHARE.COURTESY_PAY_RESTRICT_SERIAL = COURTESY_PAY_RESTRICTION.SERIAL"
			+ " WHERE "
			+ "	SHARE.SERIAL = ? ";
		try (PreparedStatement stmt = connection.prepareStatement(sql);) {
			shareSerial.set(stmt, 1);
			ResultSet rset = stmt.executeQuery();
			return rset.next();
		}
	}
	public static void main(String[] args) throws Throwable {
		System.out.println("I'm Running!");
		String javaClass = "-javaClass=" + "com.blueeaglecreditunion.script.ResetDebitCardLimits"; // class path name of the batch script you want to run
		String javaMethod = "-javaMethod=" + "runScript"; // method to call in the script class
		String database = "-database=" + "D0062T00"; // database to read from XX is the client number and YYY is the env ex: D0035T00
		String databaseHome = "-databaseHome="; // can set this if you need to read in a file into your program
		String jdbcDriver = "-jdbcDriver=" + "com.ibm.db2.jcc.DB2Driver"; // DB2 driverCoachella2017
		String jdbcURLPrefix = "-jdbcURLPrefix=" + "jdbc:db2://208.69.139.109:50000"; // DB2 URL connection to your DB
		String userName = "-userName=" + "cdenty"; // aix username
		String password = "-password=" ; // aix password
		String passwordStdInFlag = "-passwordStdInFlag=" + "";
		String userHome = "-userHome=" + "C:/Users/CDAdmin/Desktop/Test"; // location for the output folders
		String defaultThreadQueueServerCount = "-defaultThreadQueueServerCount=" + "1";
		String javaClassPath = "-javaClassPath=" + "C:/Users/CDAdmin/Documents/ResetDebitCardLimits/out/artifacts/ResetDebitCardLimits_jar/ResetDebitCardLimits.jar";
		String resultPathName = "-resultPathName=" + "C:/Users/CDAdmin/Desktop/Test/OutputReport.xml";  // default output report
		String terminatePathName = "-terminatePathName=" + "";
		String arg = "-arg="; // can pass arguments csv file names, params etc.. ex: test_csv_file.csv

		args = new String[]{
				javaClass, javaMethod, database, databaseHome, jdbcDriver,
				jdbcURLPrefix, userName, password, passwordStdInFlag, userHome,
				defaultThreadQueueServerCount, javaClassPath, resultPathName, terminatePathName, arg
		};
		com.corelationinc.script.Script.main(args);
	}
}
