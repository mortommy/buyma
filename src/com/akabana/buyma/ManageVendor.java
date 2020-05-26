package com.akabana.buyma;

import java.util.logging.Logger;

import com.akabana.db_utils.*;

/***
 * Abstract class. Classes that extend from this, manage loading data from a first source (csv or Sqlite DB) where 
 * all items are downloaded from a website, to the final Sqlite DB to staging and consolidated table.
 * 
 * @author tmorello
 *
 */
public abstract class ManageVendor 
{
	protected String sourceDb;
	protected String destinationDb;
	protected DBMS_DB sourceSqlLiteDB = null;
	protected DBMS_DB destinationSqlLiteDB = null;
	
	protected Logger logger = null;

	/**
	 * Constructor when the source db is not available, f.i. loading from csv source.
	 * @param destinationDb DB were to load the staging table
	 */
	public ManageVendor(String destinationDb)
	{
		this.destinationDb = destinationDb;		
		destinationSqlLiteDB = new SQLLite_DB(this.destinationDb);		
	}
	
	/***
	 * Constructor when the source db is not available, f.i. loading from csv source.
	 * @param destinationDb DB were to load the staging table.
	 * @param logger
	 */
	public ManageVendor(String destinationDb, Logger logger)
	{
		this(destinationDb);
		
		this.logger = logger;
	}
	
	/***
	 * Constructor when the source is also a DB.
	 * @param sourceDb DB were to find the staging source.
	 * @param destinationDb DB were to load the staging table.
	 */
	public ManageVendor(String sourceDb, String destinationDb)
	{
		this(destinationDb);
		
		this.sourceDb =sourceDb; 
		sourceSqlLiteDB = new SQLLite_DB(this.sourceDb);	
	}
	
	/***
	 * Constructor when the source is also a DB.
	 * @param sourceDb DB were to find the staging source.
	 * @param destinationDb DB were to load the staging table
	 * @param logger
	 */
	public ManageVendor(String sourceDb, String destinationDb, Logger logger)
	{
		this(sourceDb, destinationDb);
		
		this.logger = logger;	
	}	
	
	public abstract void loadStagingFromDB() throws Exception;
	public abstract void loadStagingFromCSV(String filePathName) throws Exception;
	public abstract void loadConsolidatedTable(int mode) throws Exception;
	public abstract void loadBuymaTable() throws Exception;

}//class
