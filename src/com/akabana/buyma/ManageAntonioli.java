package com.akabana.buyma;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import me.xdrop.fuzzywuzzy.FuzzySearch;
import me.xdrop.fuzzywuzzy.model.ExtractedResult;


/***
 * Extends ManageStaging. Offer two methods to load data from the first Antonioli DB, where all items are downloaded,
 * to the BuyMa DB in staging Antonioli Table; and a method to load data from the BuyMa DB Antonioli staging table 
 * to the consolidated BuyMa DB Antonioli Items. In the latter table there willl be always the Antonioli catalog
 * with status column to show if an item is new, already processed or not processable.
 * @author tmorello
 *
 */
public class ManageAntonioli extends ManageVendor
{
	private String createSTAntonioliItemsTableStatement = "CREATE TABLE ST_ANTONIOLI_ITEMS " +
			"(ITEM_ID TEXT NOT NULL," +
			"ITEM_SKU TEXT NOT NULL,"+
			"ITEM_NAME TEXT NOT NULL,"+
			"ITEM_BRAND TEXT NOT NULL,"+
			"ITEM_MODEL TEXT,"+
			"ITEM_PART TEXT,"+
			"ITEM_STYLE TEXT,"+
			"ITEM_COLOR TEXT,"+
			"ITEM_SIZE TEXT,"+
			"ITEM_DIM TEXT,"+
			"ITEM_DESCRIPTION TEXT,"+
			"ITEM_GENDER TEXT NOT NULL,"+
			"ITEM_CATEGORY TEXT,"+
			"ITEM_HIERARCHY1 TEXT,"+
			"ITEM_HIERARCHY2 TEXT,"+
			"ITEM_HIERARCHY3 TEXT,"+
			"ITEM_HIERARCHY4 TEXT,"+
			"ITEM_SEASON TEXT,"+
			"ITEM_PRICE TEXT NOT NULL,"+
			"ITEM_PRICE_CURRENCY TEXT NOT NULL,"+
			"ITEM_LINK TEXT,"+
			"ITEM_PICTURE TEXT,"+
			"ITEM_PICTURE_ALT1 TEXT,"+
			"ITEM_PICTURE_ALT2 TEXT,"+
			"ITEM_PICTURE_ALT3 TEXT,"+
			"ITEM_PICTURE_ALT4 TEXT,"+
			"ITEM_AVALAIBILITY TEXT,"+
			"ITEM_SOURCE TEXT NOT NULL,"+
			"CREATED_TIMESTAMP DATETIME NOT NULL"+
			");";
	private String createAntonioliItemsTableStatement =  "CREATE TABLE ANTONIOLI " +
			"(ITEM_ID TEXT NOT NULL," +
			"ITEM_SKU TEXT NOT NULL,"+
			"ITEM_NAME TEXT NOT NULL,"+
			"ITEM_BRAND TEXT NOT NULL,"+
			"ITEM_MODEL TEXT,"+
			"ITEM_PART TEXT,"+
			"ITEM_STYLE TEXT,"+
			"ITEM_COLOR TEXT,"+
			"ITEM_SIZE TEXT,"+
			"ITEM_DIM TEXT,"+
			"ITEM_DESCRIPTION TEXT,"+
			"ITEM_GENDER TEXT NOT NULL,"+
			"ITEM_CATEGORY TEXT,"+
			"ITEM_HIERARCHY1 TEXT,"+
			"ITEM_HIERARCHY2 TEXT,"+
			"ITEM_HIERARCHY3 TEXT,"+
			"ITEM_HIERARCHY4 TEXT,"+
			"ITEM_SEASON TEXT,"+
			"ITEM_PRICE TEXT NOT NULL,"+
			"ITEM_PRICE_CURRENCY TEXT NOT NULL,"+
			"ITEM_LINK TEXT,"+
			"ITEM_PICTURE TEXT,"+
			"ITEM_PICTURE_ALT1 TEXT,"+
			"ITEM_PICTURE_ALT2 TEXT,"+
			"ITEM_PICTURE_ALT3 TEXT,"+
			"ITEM_PICTURE_ALT4 TEXT,"+
			"ITEM_AVALAIBILITY TEXT,"+
			"ITEM_SOURCE TEXT NOT NULL,"+
			"CREATED_TIMESTAMP DATETIME NOT NULL,"+
			"STATUS INT NOT NULL,"+
			"BUYMA_STATUS INT NOT NULL,"+
			"BUYMA_STATUS_DESC	TEXT,"+
			"MODIFIED_TIMESTAMP DATETIME NOT NULL,"+
			"BUYMA_PROCESSED_TIMESTAMP DATETIME NOT NULL"+
			");";
	/***
	 * Constructor
	 * @param destinationDb where to find the destination table (ST_ANTONIOLI_ITEMS)
	 */
	public ManageAntonioli(String destinationDb)
	{
		super(destinationDb);
	}
	
	/***
	 * Constructor
	 * @param destinationDb where to find the destination table (ST_ANTONIOLI_ITEMS)
	 * @param logger
	 */
	public ManageAntonioli(String destinationDb, Logger logger)
	{
		super(destinationDb, logger);
	}
	
	/**
	 * Constructor
	 * @param sourceDb where to find the source table (ANTONIOLI_DOWNLOADED_ITEMS)
	 * @param destinationDb where to find the destination table (ST_ANTONIOLI_ITEMS)
	 */
	public ManageAntonioli(String sourceDb, String destinationDb)
	{
		super(sourceDb, destinationDb);
	}
	
	/**
	 * Constructor
	 * @param sourceDb where to find the source table (ANTONIOLI_DOWNLOADED_ITEMS)
	 * @param destinationDb where to find the destination table (ST_ANTONIOLI_ITEMS)
	 * @param Logger
	 */
	public ManageAntonioli(String sourceDb, String destinationDb, Logger logger)
	{
		super(sourceDb, destinationDb, logger);
	}

	/**
	 * Loads the items from the source DB (table ANTONIOLI_DOWNLOADED_ITEMS) to the
	 * staging table (ST_ANTONIOLI_ITEMS) of destination DB. 
	 * @throws Exception
	 */
	@Override
	public void loadStagingFromDB() throws Exception 
	{
		String sqlStatement = "";		
		try
		{	
			if(logger!=null)
				logger.log(Level.FINE, "Start loadStaging method");
			
			//connect to source db
			if(logger!=null)
				logger.log(Level.INFO, "Open the source db: "+sourceSqlLiteDB.getDbFile());
			sourceSqlLiteDB.connectToDB();
						
			//check if source table exists
			if(logger!=null)
				logger.log(Level.FINE, "Check if table ANTONIOLI_DOWNLOADED_ITEMS exists in the source DB");
			if(!sourceSqlLiteDB.tableExists("ANTONIOLI_DOWNLOADED_ITEMS"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table ANTONIOLI_DOWNLOADED_ITEMS not exist in source DB!");
				throw new Exception("Source table ANTONIOLI_DOWNLOADED_ITEMS not exists in DB "+sourceSqlLiteDB.getDbFile());
			}	
			
			if(logger!=null)
				logger.log(Level.FINE, "Check if table ERROR_WEB_PAGES exists in the source DB");
			if(!sourceSqlLiteDB.tableExists("ERROR_WEB_PAGES"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table ERROR_WEB_PAGES not exist in source DB!");
				throw new Exception("Source table ERROR_WEB_PAGES not exists in DB "+sourceSqlLiteDB.getDbFile());
			}	
			
			//get the records from the source
			if(logger!=null)
				logger.log(Level.INFO, "Get data from source table source db table ANTONIOLI_DOWNLOADED_ITEMS");
			sqlStatement = "SELECT * FROM ANTONIOLI_DOWNLOADED_ITEMS;";
			ResultSet sourceRs = sourceSqlLiteDB.executeSelect(sqlStatement);
					
			//connect to destination db
			if(logger!=null)
				logger.log(Level.INFO, "Open the destination db: "+destinationSqlLiteDB.getDbFile());
			destinationSqlLiteDB.connectToDB();			
			
			//if doesn't exists, create the destination table
			if(logger!=null)
				logger.log(Level.FINE, "Check if table ST_ANTONIOLI_ITEMS exists in destination DB");
			if(!destinationSqlLiteDB.tableExists("ST_ANTONIOLI_ITEMS"))
			{
				if(logger!=null)
					logger.log(Level.INFO, "Staging table ST_ANTONIOLI_ITEMS not exists. Create it.");
				sqlStatement = createSTAntonioliItemsTableStatement;
				destinationSqlLiteDB.executeUpdate(sqlStatement);
			}//if
			
			//clean existing data
			if(logger!=null)
				logger.log(Level.INFO, "Clean existing data from destination ST_ANTONIOLI_ITEMS");
			sqlStatement = "DELETE FROM ST_ANTONIOLI_ITEMS;";
			destinationSqlLiteDB.executeUpdate(sqlStatement);
			
			//insert items from source to staging
			if(logger!=null)
				logger.log(Level.INFO, "Insert records from source table ANTONIOLI_DOWNLOADED_ITEMS to destination ST_ANTONIOLI_ITEMS");
			while (sourceRs.next()) 
			{
				sqlStatement = "SELECT COUNT(*) FROM ST_ANTONIOLI_ITEMS "+
							   "WHERE ITEM_ID = '"+sourceRs.getString("ITEM_ID")+"' "+
							   //"AND ITEM_GENDER = '"+sourceRs.getString("ITEM_GENDER")+"' "+
							   "AND ITEM_SIZE = '"+sourceRs.getString("ITEM_SIZE")+"';";
				if(logger!=null)
					logger.log(Level.FINE, "Check if there is a record with natural key already in destination table.");
				ResultSet crs = destinationSqlLiteDB.executeSelect(sqlStatement);
				
				if(crs.getInt(1) == 0)
				{				
					sqlStatement = "INSERT INTO ST_ANTONIOLI_ITEMS (ITEM_ID,ITEM_SKU,ITEM_NAME,ITEM_BRAND,ITEM_MODEL,ITEM_PART,ITEM_STYLE,ITEM_COLOR,ITEM_SIZE,ITEM_DIM,ITEM_DESCRIPTION,ITEM_GENDER,ITEM_CATEGORY,ITEM_HIERARCHY1,ITEM_HIERARCHY2,ITEM_HIERARCHY3,ITEM_HIERARCHY4,ITEM_SEASON,ITEM_PRICE,ITEM_PRICE_CURRENCY,ITEM_LINK,ITEM_PICTURE,ITEM_PICTURE_ALT1,ITEM_PICTURE_ALT2,ITEM_PICTURE_ALT3,ITEM_PICTURE_ALT4,ITEM_AVALAIBILITY,ITEM_SOURCE,CREATED_TIMESTAMP) " +
							"VALUES('"+sourceRs.getString("ITEM_ID")+"','"+sourceRs.getString("ITEM_SKU")+"','"+sourceRs.getString("ITEM_NAME")+"','"+sourceRs.getString("ITEM_BRAND")+"','"+
							sourceRs.getString("ITEM_MODEL")+"','"+sourceRs.getString("ITEM_PART")+"','"+sourceRs.getString("ITEM_STYLE")+"','"+sourceRs.getString("ITEM_COLOR")+"','"+sourceRs.getString("ITEM_SIZE")+"','"+
							sourceRs.getString("ITEM_DIM")+"','"+sourceRs.getString("ITEM_DESCRIPTION").replace("'"," ")+"','"+sourceRs.getString("ITEM_GENDER")+"','"+sourceRs.getString("ITEM_CATEGORY")+"','"+
							sourceRs.getString("ITEM_HIERARCHY1")+"','"+sourceRs.getString("ITEM_HIERARCHY2")+"','"+sourceRs.getString("ITEM_HIERARCHY3")+"','"+sourceRs.getString("ITEM_HIERARCHY4")+"','"+
							sourceRs.getString("ITEM_SEASON")+"','"+sourceRs.getString("ITEM_PRICE")+"','"+sourceRs.getString("ITEM_PRICE_CURRENCY")+"','"+sourceRs.getString("ITEM_LINK")+"','"+
							sourceRs.getString("ITEM_PICTURE")+"','"+sourceRs.getString("ITEM_PICTURE_ALT1")+"','"+sourceRs.getString("ITEM_PICTURE_ALT2")+"','"+sourceRs.getString("ITEM_PICTURE_ALT3")+"','"+sourceRs.getString("ITEM_PICTURE_ALT4")+"','"+
							sourceRs.getString("ITEM_AVALAIBILITY")+"','"+sourceRs.getString("ITEM_SOURCE")+"','"+sourceRs.getString("CREATED_TIMESTAMP")+"'"+
							");";
					if(logger!=null)
						logger.log(Level.FINE, "Run insert statement.");
					destinationSqlLiteDB.executeUpdate(sqlStatement);
				}
				else
				{
					if(logger!=null)
						logger.log(Level.FINE, "Found a natural key ("+sourceRs.getString("ITEM_ID")+", "+sourceRs.getString("ITEM_GENDER")+", "+sourceRs.getString("ITEM_SIZE")+") already in destination table, skip insert.");
				}
					
			}//while			
			
			//get the records from the source
			if(logger!=null)
				logger.log(Level.INFO, "Get data from source table source db table ERROR_WEB_PAGES");
			sqlStatement = "SELECT * FROM ERROR_WEB_PAGES;";
			sourceRs = sourceSqlLiteDB.executeSelect(sqlStatement);
					
			//if doesn't exists, create the destination table
			if(logger!=null)
				logger.log(Level.FINE, "Check if table ERROR_WEB_PAGES exists in destination DB");
			if(!destinationSqlLiteDB.tableExists("ERROR_WEB_PAGES"))
			{
				if(logger!=null)
					logger.log(Level.INFO, "Staging table ERROR_WEB_PAGES not exists. Create it.");
				sqlStatement = "CREATE TABLE ERROR_WEB_PAGES ( "+
						"LINK TEXT);";
				destinationSqlLiteDB.executeUpdate(sqlStatement);
			}//if
			
			//clean existing data
			if(logger!=null)
				logger.log(Level.INFO, "Clean existing data from destination ST_ANTONIOLI_ITEMS");
			sqlStatement = "DELETE FROM ERROR_WEB_PAGES;";
			destinationSqlLiteDB.executeUpdate(sqlStatement);
			
			//insert items from source to staging
			if(logger!=null)
				logger.log(Level.INFO, "Insert records from source table ERROR_WEB_PAGES to destination ERROR_WEB_PAGES");
			while (sourceRs.next()) 
			{
							
				sqlStatement = "INSERT INTO ERROR_WEB_PAGES (LINK) " +
						"VALUES('"+sourceRs.getString("LINK")+"');";
				if(logger!=null)
					logger.log(Level.FINE, "Run insert statement.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);					
			}//while				
			
			if(logger!=null)
				logger.log(Level.FINE, "Close connections to DBs.");
			sourceRs.close();
			sourceSqlLiteDB.closeDBConnection();
			destinationSqlLiteDB.closeDBConnection();
			
			if(logger!=null)
				logger.log(Level.FINE, "End loadStaging method.");
		}
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
		}		
	}//loadStaging
	
	/**
	 * Loads the items from a CSV file to the
	 * staging table (ST_ANTONIOLI_ITEMS) of destination DB. 
	 * @filePathName name and path of csv file
	 * @throws Exception
	 */
	@Override
	public void loadStagingFromCSV(String filePathName) throws Exception
	{
		//TO DO
	}
	
	/**
	 * Inserts new items, or marks items to be deleted in table (ANTONIOLI) using the staging table (ST_ANTONIOLI_ITEMS)
	 * The table (ANTONIOLI) is the source for the final table used to update buyma, and contains all the items managed so far.
	 * @throws Exception
	 */
	@Override
	public void loadConsolidatedTable(int mode) throws Exception
	{
		String sqlStatement = "";		
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start loadConsolidatedTable method.");
			
			//connect to destination db
			if(logger!=null)
				logger.log(Level.INFO, "Open the destination db: "+destinationSqlLiteDB.getDbFile());
			destinationSqlLiteDB.connectToDB();	
			
			//check if source table exists
			if(logger!=null)
				logger.log(Level.FINE, "Check if table ST_ANTONIOLI_ITEMS exists");
			if(!destinationSqlLiteDB.tableExists("ST_ANTONIOLI_ITEMS"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table ST_ANTONIOLI_ITEMS not exist");
				throw new Exception("Source table ST_ANTONIOLI_ITEMS not exists in DB "+destinationSqlLiteDB.getDbFile());
			}
			
			//if doesn't exists, create the destination table
			if(logger!=null)
				logger.log(Level.FINE, "Check if table ANTONIOLI exists");
			if(!destinationSqlLiteDB.tableExists("ANTONIOLI"))
			{
				if(logger!=null)
					logger.log(Level.INFO, "Table ANTONIOLI not exists. Create it.");
				sqlStatement = createAntonioliItemsTableStatement;
				destinationSqlLiteDB.executeUpdate(sqlStatement);
			}//if
			
			//INSERT first the NEW items that are in staging and not in destination
			//The new records status will be to NEW and TO_BE_PROCESSED
			sqlStatement = "INSERT INTO ANTONIOLI (ITEM_ID,ITEM_SKU,ITEM_NAME,ITEM_BRAND,ITEM_MODEL,ITEM_PART,ITEM_STYLE,ITEM_COLOR,ITEM_SIZE,ITEM_DIM,ITEM_DESCRIPTION,ITEM_GENDER,ITEM_CATEGORY,ITEM_HIERARCHY1,ITEM_HIERARCHY2,ITEM_HIERARCHY3,ITEM_HIERARCHY4,ITEM_SEASON,ITEM_PRICE,ITEM_PRICE_CURRENCY,ITEM_LINK,ITEM_PICTURE,ITEM_PICTURE_ALT1,ITEM_PICTURE_ALT2,ITEM_PICTURE_ALT3,ITEM_PICTURE_ALT4,ITEM_AVALAIBILITY,ITEM_SOURCE,CREATED_TIMESTAMP,STATUS,BUYMA_STATUS,BUYMA_STATUS_DESC,MODIFIED_TIMESTAMP,BUYMA_PROCESSED_TIMESTAMP) "+
						   "SELECT st.ITEM_ID,st.ITEM_SKU,st.ITEM_NAME,st.ITEM_BRAND,st.ITEM_MODEL,st.ITEM_PART,st.ITEM_STYLE,st.ITEM_COLOR,st.ITEM_SIZE,st.ITEM_DIM,st.ITEM_DESCRIPTION,st.ITEM_GENDER,st.ITEM_CATEGORY,st.ITEM_HIERARCHY1,st.ITEM_HIERARCHY2,st.ITEM_HIERARCHY3,st.ITEM_HIERARCHY4,st.ITEM_SEASON,st.ITEM_PRICE,st.ITEM_PRICE_CURRENCY,st.ITEM_LINK,st.ITEM_PICTURE,st.ITEM_PICTURE_ALT1,st.ITEM_PICTURE_ALT2,st.ITEM_PICTURE_ALT3,st.ITEM_PICTURE_ALT4,st.ITEM_AVALAIBILITY,st.ITEM_SOURCE,st.CREATED_TIMESTAMP,"+
						   +StagingRecordStatus.NEW+","+StagingBuymaStatus.TO_BE_PROCESSED+",\"\",st.CREATED_TIMESTAMP,'1900-01-01 00:00:00:000'"+
						   "FROM ST_ANTONIOLI_ITEMS st "+
						   "WHERE st.ITEM_ID NOT IN "+
						   "(SELECT dt.ITEM_ID "+
						   "FROM ANTONIOLI dt) "+
						   "AND st.ITEM_LINK NOT IN "+
						   "(SELECT LINK FROM ERROR_WEB_PAGES);";
			if(logger!=null)
				logger.log(Level.INFO, "Insert into ANTONIOLI the new items found in staging.");
			destinationSqlLiteDB.executeUpdate(sqlStatement);	
			
			if(mode == LoadDataMode.INSERT_UPDATE)
			{
				//mark items in the consolidate table as DELETED, NOT ANYMORE PRESENT in staging
				//the check is only on items that should be present in BuyMa (status NEW, PROCESSED or MODIFIED, PROCESSED)
				//the new records status will be to DELETED and TO_BE_PROCESSED
				Timestamp timestamp = new Timestamp(System.currentTimeMillis());
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sss");
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET STATUS="+StagingRecordStatus.DELETED+", "+
								"MODIFIED_TIMESTAMP='"+sdf.format(timestamp)+"', "+
								"BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+" "+
								"WHERE ITEM_ID NOT IN "+
								"(SELECT st.ITEM_ID "+
								"FROM ST_ANTONIOLI_ITEMS st)"+
								"AND (STATUS="+StagingRecordStatus.NEW+" "+
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+")"+
								"OR "+
								"(STATUS="+StagingRecordStatus.MODIFIED+" "+
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+
								"AND ITEM_HIERARCHY2 <> 'Size (S/M/L)';";
				if(logger!=null)
					logger.log(Level.INFO, "Mark in ANTONIOLI as deleted items not found in staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//UPDATE and mark items with the records from staging where some FIELDS CHANGED
				//the check is only on items that should be present in BuyMa (status NEW, PROCESSED or MODIFIED, PROCESSED)
				
				/*
				//sqlite allows the update of one field at time
				
				//modified date
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET MODIFIED_TIMESTAMP= IFNULL((select '"+sdf.format(timestamp)+"' "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"("+
								"s.ITEM_NAME <> ANTONIOLI.ITEM_NAME OR "+ 
								//"s.ITEM_BRAND <> ANTONIOLI.ITEM_BRAND OR "+ 
								"s.ITEM_DESCRIPTION <> ANTONIOLI.ITEM_DESCRIPTION OR "+ 
								//"s.ITEM_CATEGORY <> ANTONIOLI.ITEM_CATEGORY OR "+ 
								//"s.ITEM_HIERARCHY1 <> ANTONIOLI.ITEM_HIERARCHY1 OR "+ 
								//"s.ITEM_HIERARCHY2 <> ANTONIOLI.ITEM_HIERARCHY2 OR "+ 
								//"s.ITEM_HIERARCHY3 <> ANTONIOLI.ITEM_HIERARCHY3 OR "+ 
								//"s.ITEM_HIERARCHY4 <> ANTONIOLI.ITEM_HIERARCHY4 OR "+ 
								"s.ITEM_PRICE <> ANTONIOLI.ITEM_PRICE OR "+ 
								"s.ITEM_PRICE_CURRENCY <> ANTONIOLI.ITEM_PRICE_CURRENCY OR "+ 
								"s.ITEM_PICTURE <> ANTONIOLI.ITEM_PICTURE OR "+ 
								"s.ITEM_PICTURE_ALT1 <> ANTONIOLI.ITEM_PICTURE_ALT1 OR "+ 
								"s.ITEM_PICTURE_ALT2 <> ANTONIOLI.ITEM_PICTURE_ALT2 OR "+ 
								"s.ITEM_PICTURE_ALT3 <> ANTONIOLI.ITEM_PICTURE_ALT3 OR "+ 
								"s.ITEM_PICTURE_ALT4 <> ANTONIOLI.ITEM_PICTURE_ALT4 OR "+ 
								"s.ITEM_AVALAIBILITY <> ANTONIOLI.ITEM_AVALAIBILITY) "+
								"),ANTONIOLI.MODIFIED_TIMESTAMP) "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+ 
								"OR "+ 
								"(STATUS="+StagingRecordStatus.MODIFIED+" " + 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+");";
				if(logger!=null)
					logger.log(Level.INFO, "Update modified date of ANTONIOLI items changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//status
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET STATUS= IFNULL((select "+StagingRecordStatus.MODIFIED+" "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"("+
								"s.ITEM_NAME <> ANTONIOLI.ITEM_NAME OR "+ 
								//"s.ITEM_BRAND <> ANTONIOLI.ITEM_BRAND OR "+ 
								"s.ITEM_DESCRIPTION <> ANTONIOLI.ITEM_DESCRIPTION OR "+ 
								//"s.ITEM_CATEGORY <> ANTONIOLI.ITEM_CATEGORY OR "+ 
								//"s.ITEM_HIERARCHY1 <> ANTONIOLI.ITEM_HIERARCHY1 OR "+ 
								//"s.ITEM_HIERARCHY2 <> ANTONIOLI.ITEM_HIERARCHY2 OR "+ 
								//"s.ITEM_HIERARCHY3 <> ANTONIOLI.ITEM_HIERARCHY3 OR "+ 
								//"s.ITEM_HIERARCHY4 <> ANTONIOLI.ITEM_HIERARCHY4 OR "+ 
								"s.ITEM_PRICE <> ANTONIOLI.ITEM_PRICE OR "+ 
								"s.ITEM_PRICE_CURRENCY <> ANTONIOLI.ITEM_PRICE_CURRENCY OR "+ 
								"s.ITEM_PICTURE <> ANTONIOLI.ITEM_PICTURE OR "+ 
								"s.ITEM_PICTURE_ALT1 <> ANTONIOLI.ITEM_PICTURE_ALT1 OR "+ 
								"s.ITEM_PICTURE_ALT2 <> ANTONIOLI.ITEM_PICTURE_ALT2 OR "+ 
								"s.ITEM_PICTURE_ALT3 <> ANTONIOLI.ITEM_PICTURE_ALT3 OR "+ 
								"s.ITEM_PICTURE_ALT4 <> ANTONIOLI.ITEM_PICTURE_ALT4 OR "+ 
								"s.ITEM_AVALAIBILITY <> ANTONIOLI.ITEM_AVALAIBILITY) "+
								"),ANTONIOLI.STATUS) "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+ 
								"OR "+ 
								"(STATUS="+StagingRecordStatus.MODIFIED+" " + 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+");";
				if(logger!=null)
					logger.log(Level.INFO, "Update status of ANTONIOLI items changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//buyma status
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET BUYMA_STATUS= IFNULL((select "+StagingBuymaStatus.TO_BE_PROCESSED+" "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"("+
								"s.ITEM_NAME <> ANTONIOLI.ITEM_NAME OR "+ 
								"s.ITEM_BRAND <> ANTONIOLI.ITEM_BRAND OR "+ 
								"s.ITEM_DESCRIPTION <> ANTONIOLI.ITEM_DESCRIPTION OR "+ 
								"s.ITEM_CATEGORY <> ANTONIOLI.ITEM_CATEGORY OR "+ 
								"s.ITEM_HIERARCHY1 <> ANTONIOLI.ITEM_HIERARCHY1 OR "+ 
								"s.ITEM_HIERARCHY2 <> ANTONIOLI.ITEM_HIERARCHY2 OR "+ 
								"s.ITEM_HIERARCHY3 <> ANTONIOLI.ITEM_HIERARCHY3 OR "+ 
								"s.ITEM_HIERARCHY4 <> ANTONIOLI.ITEM_HIERARCHY4 OR "+ 
								"s.ITEM_PRICE <> ANTONIOLI.ITEM_PRICE OR "+ 
								"s.ITEM_PRICE_CURRENCY <> ANTONIOLI.ITEM_PRICE_CURRENCY OR "+ 
								"s.ITEM_PICTURE <> ANTONIOLI.ITEM_PICTURE OR "+ 
								"s.ITEM_PICTURE_ALT1 <> ANTONIOLI.ITEM_PICTURE_ALT1 OR "+ 
								"s.ITEM_PICTURE_ALT2 <> ANTONIOLI.ITEM_PICTURE_ALT2 OR "+ 
								"s.ITEM_PICTURE_ALT3 <> ANTONIOLI.ITEM_PICTURE_ALT3 OR "+ 
								"s.ITEM_PICTURE_ALT4 <> ANTONIOLI.ITEM_PICTURE_ALT4 OR "+ 
								"s.ITEM_AVALAIBILITY <> ANTONIOLI.ITEM_AVALAIBILITY) "+
								"),ANTONIOLI.STATUS) "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+ 
								"OR "+ 
								"(STATUS="+StagingRecordStatus.MODIFIED+" " + 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+");";
				if(logger!=null)
					logger.log(Level.INFO, "Update status of ANTONIOLI items changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				/*
				//brand
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET ITEM_BRAND= IFNULL((select s.ITEM_BRAND "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"s.ITEM_BRAND <> ANTONIOLI.ITEM_BRAND "+
								"),ANTONIOLI.ITEM_BRAND) "+
								"WHERE STATUS<>"+StagingRecordStatus.DELETED+" "+
								";";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_BRAND of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//description
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET ITEM_DESCRIPTION= IFNULL((select s.ITEM_DESCRIPTION "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"s.ITEM_DESCRIPTION <> ANTONIOLI.ITEM_DESCRIPTION "+
								"),ANTONIOLI.ITEM_DESCRIPTION) "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+ 
								"OR "+ 
								"(STATUS="+StagingRecordStatus.MODIFIED+" " + 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+");";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_DESCRIPTION of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				/*
				//category
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET ITEM_CATEGORY= IFNULL((select s.ITEM_CATEGORY "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"s.ITEM_CATEGORY <> ANTONIOLI.ITEM_CATEGORY "+
								"),ANTONIOLI.ITEM_CATEGORY) "+
								"WHERE STATUS<>"+StagingRecordStatus.DELETED+" "+
								";";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_CATEGORY of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//Hierarchy1
				sqlStatement = "UPDATE ANTONIOLI "+
							"SET ITEM_HIERARCHY1= IFNULL((select s.ITEM_HIERARCHY1 "+
							"FROM ST_ANTONIOLI_ITEMS s "+ 
							"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
							"s.ITEM_HIERARCHY1 <> ANTONIOLI.ITEM_HIERARCHY1 "+
							"),ANTONIOLI.ITEM_HIERARCHY1) "+
							"WHERE STATUS<>"+StagingRecordStatus.DELETED+" "+
							";";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_HIERARCHY1 of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//Hierarchy2
				sqlStatement = "UPDATE ANTONIOLI "+
							"SET ITEM_HIERARCHY2= IFNULL((select s.ITEM_HIERARCHY2 "+
							"FROM ST_ANTONIOLI_ITEMS s "+ 
							"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
							"s.ITEM_HIERARCHY2 <> ANTONIOLI.ITEM_HIERARCHY2 "+
							"),ANTONIOLI.ITEM_HIERARCHY2) "+
							"WHERE STATUS<>"+StagingRecordStatus.DELETED+" "+
							";";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_HIERARCHY2 of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//Hierarchy3
				sqlStatement = "UPDATE ANTONIOLI "+
							"SET ITEM_HIERARCHY3= IFNULL((select s.ITEM_HIERARCHY3 "+
							"FROM ST_ANTONIOLI_ITEMS s "+ 
							"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
							"s.ITEM_HIERARCHY3 <> ANTONIOLI.ITEM_HIERARCHY3 "+
							"),ANTONIOLI.ITEM_HIERARCHY3) "+
							"WHERE STATUS<>"+StagingRecordStatus.DELETED+" "+
							";";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_HIERARCHY3 of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//Hierarchy4
				sqlStatement = "UPDATE ANTONIOLI "+
							"SET ITEM_HIERARCHY4= IFNULL((select s.ITEM_HIERARCHY4 "+
							"FROM ST_ANTONIOLI_ITEMS s "+ 
							"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
							"s.ITEM_HIERARCHY4 <> ANTONIOLI.ITEM_HIERARCHY4 "+
							"),ANTONIOLI.ITEM_HIERARCHY4) "+
							"WHERE STATUS<>"+StagingRecordStatus.DELETED+" "+
							";";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_HIERARCHY4 of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//price
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET ITEM_PRICE= IFNULL((select s.ITEM_PRICE "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"s.ITEM_PRICE <> ANTONIOLI.ITEM_PRICE "+
								"),ANTONIOLI.ITEM_PRICE) "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+ 
								"OR "+ 
								"(STATUS="+StagingRecordStatus.MODIFIED+" " + 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+");";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_PRICE of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//price_currency
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET ITEM_PRICE_CURRENCY= IFNULL((select s.ITEM_PRICE_CURRENCY "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"s.ITEM_PRICE_CURRENCY <> ANTONIOLI.ITEM_PRICE_CURRENCY "+
								"),ANTONIOLI.ITEM_PRICE_CURRENCY) "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+ 
								"OR "+ 
								"(STATUS="+StagingRecordStatus.MODIFIED+" " + 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+");";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_PRICE_CURRENCY of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//picture
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET ITEM_PICTURE= IFNULL((select s.ITEM_PICTURE "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"s.ITEM_PICTURE <> ANTONIOLI.ITEM_PICTURE "+
								"),ANTONIOLI.ITEM_PICTURE) "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+ 
								"OR "+ 
								"(STATUS="+StagingRecordStatus.MODIFIED+" " + 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+");";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_PICTURE of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//picture_alt1
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET ITEM_PICTURE_ALT1= IFNULL((select s.ITEM_PICTURE_ALT1 "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"s.ITEM_PICTURE_ALT1 <> ANTONIOLI.ITEM_PICTURE_ALT1 "+
								"),ANTONIOLI.ITEM_PICTURE_ALT1) "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+ 
								"OR "+ 
								"(STATUS="+StagingRecordStatus.MODIFIED+" " + 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+");";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_PICTURE_ALT1 of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//picture_alt2
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET ITEM_PICTURE_ALT2= IFNULL((select s.ITEM_PICTURE_ALT2 "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"s.ITEM_PICTURE_ALT2 <> ANTONIOLI.ITEM_PICTURE_ALT2 "+
								"),ANTONIOLI.ITEM_PICTURE_ALT2) "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+ 
								"OR "+ 
								"(STATUS="+StagingRecordStatus.MODIFIED+" " + 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+");";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_PICTURE_ALT2 of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//picture_alt3
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET ITEM_PICTURE_ALT3= IFNULL((select s.ITEM_PICTURE_ALT3 "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"s.ITEM_PICTURE_ALT3 <> ANTONIOLI.ITEM_PICTURE_ALT3 "+
								"),ANTONIOLI.ITEM_PICTURE_ALT3) "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+ 
								"OR "+ 
								"(STATUS="+StagingRecordStatus.MODIFIED+" " + 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+");";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_PICTURE_ALT3 of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//picture_alt4
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET ITEM_PICTURE_ALT4= IFNULL((select s.ITEM_PICTURE_ALT4 "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"s.ITEM_PICTURE_ALT4 <> ANTONIOLI.ITEM_PICTURE_ALT4 "+
								"),ANTONIOLI.ITEM_PICTURE_ALT4) "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+ 
								"OR "+ 
								"(STATUS="+StagingRecordStatus.MODIFIED+" " + 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+");";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_PICTURE_ALT4 of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				//availability
				sqlStatement = "UPDATE ANTONIOLI "+
								"SET ITEM_AVALAIBILITY= IFNULL((select s.ITEM_AVALAIBILITY "+
								"FROM ST_ANTONIOLI_ITEMS s "+ 
								"WHERE ANTONIOLI.ITEM_ID = s.ITEM_ID AND "+
								"s.ITEM_AVALAIBILITY <> ANTONIOLI.ITEM_AVALAIBILITY "+
								"),ANTONIOLI.ITEM_AVALAIBILITY) "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+") "+ 
								"OR "+ 
								"(STATUS="+StagingRecordStatus.MODIFIED+" " + 
								"AND BUYMA_STATUS="+StagingBuymaStatus.PROCESSED+");";
				if(logger!=null)
					logger.log(Level.INFO, "Update ITEM_AVALAIBILITY of ANTONIOLI items where value changed from the staging.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				 */	
			}
			if(logger!=null)
				logger.log(Level.FINE, "End loadConsolidatedTable method.");
		}
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
		}
	}//loadConsolidatedTable
	
	/***
	 * Starting from the source table (ANTONIOLI) loads data to BuyMa tables (ANTONIOLI_ITEMS_FOR_BUYMA; 
	 * ANTONIOLI_COLOR_SIZES_FOR_BUYMA) using current items loaded in BuyMa (thanks to tables BUYMA_ITEMS
	 * and BUYMA_COLOR_SIZES)
	 * @throws Exception 
	 */
	@Override
	public void loadBuymaTable() throws Exception
	{
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start loadBuyMaTableFromAntonioli method.");
			
			//connect to db
			if(logger!=null)
				logger.log(Level.INFO, "Open the db: "+destinationSqlLiteDB.getDbFile());
			destinationSqlLiteDB.connectToDB();
			
			//check if source table exists
			if(logger!=null)
				logger.log(Level.FINE, "Check if table ANTONIOLI exists");
			if(!destinationSqlLiteDB.tableExists("ANTONIOLI"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table ANTONIOLI not exist");
				throw new Exception("Source table ANTONIOLI not exists in DB "+destinationSqlLiteDB.getDbFile());
			}
			
			//check if destination tables exists
			if(logger!=null)
				logger.log(Level.FINE, "Check if table ANTONIOLI_ITEMS_TO_BUYMA exists");
			if(!destinationSqlLiteDB.tableExists("ANTONIOLI_ITEMS_TO_BUYMA"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table ANTONIOLI_ITEMS_TO_BUYMA not exist");
				throw new Exception("Source table ANTONIOLI_ITEMS_TO_BUYMA not exists in DB "+destinationSqlLiteDB.getDbFile());
			}
			if(logger!=null)
				logger.log(Level.FINE, "Check if table ANTONIOLI_COLOR_SIZES_TO_BUYMA exists");
			if(!destinationSqlLiteDB.tableExists("ANTONIOLI_COLOR_SIZES_TO_BUYMA"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table ANTONIOLI_COLOR_SIZES_TO_BUYMA not exist");
				throw new Exception("Source table ANTONIOLI_COLOR_SIZES_TO_BUYMA not exists in DB "+destinationSqlLiteDB.getDbFile());
			}
			
			//check if support tables exists
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_ITEMS exists");
			if(!destinationSqlLiteDB.tableExists("BUYMA_ITEMS"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_ITEMS not exist");
				throw new Exception("Source table BUYMA_ITEMS not exists in DB "+destinationSqlLiteDB.getDbFile());
			}
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_COLOR_SIZES exists");
			if(!destinationSqlLiteDB.tableExists("BUYMA_COLOR_SIZES"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_COLOR_SIZES not exist");
				throw new Exception("Source table BUYMA_COLOR_SIZES not exists in DB "+destinationSqlLiteDB.getDbFile());
			}
			
			//truncate the destination tables
			String sqlStatement = "DELETE FROM ANTONIOLI_COLOR_SIZES_TO_BUYMA;";
			if(logger!=null)
				logger.log(Level.INFO, "Trucate the table ANTONIOLI_COLOR_SIZES_TO_BUYMA");
			destinationSqlLiteDB.executeUpdate(sqlStatement);
			
			sqlStatement = "DELETE FROM ANTONIOLI_ITEMS_TO_BUYMA;";
			if(logger!=null)
				logger.log(Level.INFO, "Trucate the table ANTONIOLI_ITEMS_TO_BUYMA");
			destinationSqlLiteDB.executeUpdate(sqlStatement);
			
			//MANAGE ITEM TO BE DELETED
			sqlStatement = ""+
					"INSERT INTO ANTONIOLI_COLOR_SIZES_TO_BUYMA ("+            	
					"\"商品ID\","+
					"\"商品管理番号\","+
					"\"並び順\","+
					"\"サイズ名称\","+
					"\"サイズ単位\","+
					"\"検索用サイズ\","+
					"\"色名称\","+
					"\"色系統\","+
					"\"在庫ステータス\","+
					"\"手元に在庫あり数量\","+
					"\"着丈\","+
					"\"肩幅\","+
					"\"胸囲\","+
					"\"袖丈\","+
					"\"ウエスト\","+
					"\"ヒップ\","+
					"\"総丈\","+
					"\"幅\","+
					"\"股上\","+
					"\"股下\","+
					"\"もも周り\","+
					"\"すそ周り\","+
					"\"スカート丈\","+
					"\"トップ\","+
					"\"アンダー\","+
					"\"高さ\","+
					"\"ヒール高\","+
					"\"つつ周り\","+
					"\"足幅\","+
					"\"マチ\","+
					"\"持ち手\","+
					"\"ストラップ\","+
					"\"奥行\","+
					"\"縦\","+
					"\"横\","+
					"\"厚み\","+
					"\"長さ\","+
					"\"トップ縦\","+
					"\"トップ横\","+
					"\"円周\","+
					"\"手首周り\","+
					"\"文字盤縦\","+
					"\"文字盤横\","+
					"\"フレーム縦\","+
					"\"フレーム横\","+
					"\"レンズ縦\","+
					"\"レンズ横\","+
					"\"テンプル\","+
					"\"全長\","+
					"\"最大幅\","+
					"\"頭周り\","+
					"\"つば\","+
					"\"直径\")"+
					" SELECT "+
	            	"buy.\"商品ID\","+
	            	"buy.\"商品管理番号\","+
	            	"buy.\"並び順\","+
	            	"buy.\"サイズ名称\","+
	            	"buy.\"サイズ単位\","+
	            	"buy.\"検索用サイズ\","+
	            	"buy.\"色名称\","+
	            	"buy.\"色系統\","+
	            	"'0',"+ //out of stock
	            	"'',"+ //no stock quantity
	            	"buy.\"着丈\","+
	            	"buy.\"肩幅\","+
	            	"buy.\"胸囲\","+
	            	"buy.\"袖丈\","+
	            	"buy.\"ウエスト\","+
	            	"buy.\"ヒップ\","+
	            	"buy.\"総丈\","+
	            	"buy.\"幅\","+
	            	"buy.\"股上\","+
	            	"buy.\"股下\","+
	            	"buy.\"もも周り\","+
	            	"buy.\"すそ周り\","+
	            	"buy.\"スカート丈\","+
	            	"buy.\"トップ\","+
	            	"buy.\"アンダー\","+
	            	"buy.\"高さ\","+
	            	"buy.\"ヒール高\","+
	            	"buy.\"つつ周り\","+
	            	"buy.\"足幅\","+
	            	"buy.\"マチ\","+
	            	"buy.\"持ち手\","+
	            	"buy.\"ストラップ\","+
	            	"buy.\"奥行\","+
	            	"buy.\"縦\","+
	            	"buy.\"横\","+
	            	"buy.\"厚み\","+
	            	"buy.\"長さ\","+
	            	"buy.\"トップ縦\","+
	            	"buy.\"トップ横\","+
	            	"buy.\"円周\","+
	            	"buy.\"手首周り\","+
	            	"buy.\"文字盤縦\","+
	            	"buy.\"文字盤横\","+
	            	"buy.\"フレーム縦\","+
	            	"buy.\"フレーム横\","+
	            	"buy.\"レンズ縦\","+
	            	"buy.\"レンズ横\","+
	            	"buy.\"テンプル\","+
	            	"buy.\"全長\","+
	            	"buy.\"最大幅\","+
	            	"buy.\"頭周り\","+
	            	"buy.\"つば\","+
	            	"buy.\"直径\" "+
	            	"FROM ANTONIOLI ant JOIN BUYMA_COLOR_SIZES buy ON ant.ITEM_ID = buy.商品管理番号  " + 
	            	" AND buy.サイズ名称 = "+
	            	" CASE "+
	            	"  WHEN ant.ITEM_HIERARCHY2 = 'Size (EU)' THEN 'EU'||REPLACE(ant.ITEM_SIZE,'½','.5') "+
	            	"  WHEN ant.ITEM_HIERARCHY2 = 'Size (FRANCE)' THEN 'FR'||REPLACE(ant.ITEM_SIZE,'½','.5') "+
	            	"  WHEN ant.ITEM_HIERARCHY2 = 'Size (USA)' THEN 'US'||REPLACE(ant.ITEM_SIZE,'½','.5') "+
	            	"  WHEN ant.ITEM_HIERARCHY2 = 'Size (UK)' THEN 'UK'||REPLACE(ant.ITEM_SIZE,'½','.5') "+
	            	"  WHEN ant.ITEM_HIERARCHY2 = 'Size (ITALY)' THEN 'IT'||REPLACE(ant.ITEM_SIZE,'½','.5') "+
	            	"  WHEN ant.ITEM_HIERARCHY2 = 'Size (JAPAN)' THEN 'JP'||REPLACE(ant.ITEM_SIZE,'½','.5') "+
	            	"  ELSE REPLACE(ant.ITEM_SIZE,'½','.5') "+
	            	"  END "+
	             	"WHERE ant.STATUS = "+StagingRecordStatus.DELETED+" "+
	                "AND ant.BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+";";
			if(logger!=null)
				logger.log(Level.INFO, "Insert into the table ANTONIOLI_COLOR_SIZES_TO_BUYMA items with 0 stock.");
			destinationSqlLiteDB.executeUpdate(sqlStatement);
			
			//check if all sizes of an item have been deleted
			//in that case we have to deactivate the item
			/* at the moment deactivated 2020-04-21, Murakami way works
			sqlStatement = " "+
					"INSERT INTO ANTONIOLI_ITEMS_TO_BUYMA (" +
					"\"商品ID\","+
					"\"商品管理番号\","+
					"\"コントロール\","+
					"\"公開ステータス\","+
					"\"商品名\","+
					"\"ブランド\","+
					"\"ブランド名\","+
					"\"モデル\","+
					"\"カテゴリ\","	+
					"\"シーズン\","+
					"\"テーマ\","+
					"\"単価\","+
					"\"買付可数量\","	+
					"\"購入期限\","+
					"\"参考価格/通常出品価格\","+
					"\"参考価格\","+
					"\"商品コメント\","+
					"\"色サイズ補足\","+
					"\"タグ\","+
					"\"配送方法\","+
					"\"買付エリア\","+
					"\"買付都市\","+
					"\"買付ショップ\","+
					"\"発送エリア\","+
					"\"発送都市\","+
					"\"関税込み\","+
					"\"出品メモ\","+
					"\"商品イメージ1\","+
					"\"商品イメージ2\","+
					"\"商品イメージ3\","+
					"\"商品イメージ4\","+
					"\"商品イメージ5\","+
					"\"商品イメージ6\","+
					"\"商品イメージ7\","+
					"\"商品イメージ8\","+
					"\"商品イメージ9\","+
					"\"商品イメージ10\","+
					"\"商品イメージ11\","+
					"\"商品イメージ12\","+
					"\"商品イメージ13\","+
					"\"商品イメージ14\","+
					"\"商品イメージ15\","+
					"\"商品イメージ16\","+
					"\"商品イメージ17\","+
					"\"商品イメージ18\","+
					"\"商品イメージ19\","+
					"\"商品イメージ20\","+
					"\"ブランド型番1\","+
					"\"ブランド型番2\","+
					"\"ブランド型番3\","+
					"\"ブランド型番4\","+
					"\"ブランド型番5\","+
					"\"ブランド型番6\","+
					"\"ブランド型番7\","+
					"\"ブランド型番8\","+
					"\"ブランド型番9\","+
					"\"ブランド型番10\","+
					"\"ブランド型番識別メモ1\","+
					"\"ブランド型番識別メモ2\","+
					"\"ブランド型番識別メモ3\","+
					"\"ブランド型番識別メモ4\","+
					"\"ブランド型番識別メモ5\","+
					"\"ブランド型番識別メモ6\","+
					"\"ブランド型番識別メモ7\","+
					"\"ブランド型番識別メモ8\","+
					"\"ブランド型番識別メモ9\","+
					"\"ブランド型番識別メモ10\","+
					"\"買付先名1\","+
					"\"買付先名2\","+
					"\"買付先名3\","+
					"\"買付先名4\","+
					"\"買付先名5\","+
					"\"買付先名6\","+
					"\"買付先名7\","+
					"\"買付先名8\","+
					"\"買付先名9\","+
					"\"買付先名10\","	+
					"\"買付先名11\","	+
					"\"買付先名12\","	+
					"\"買付先名13\","	+
					"\"買付先名14\","	+
					"\"買付先名15\","	+
					"\"買付先URL1\","+
					"\"買付先URL2\","+
					"\"買付先URL3\","+
					"\"買付先URL4\","+
					"\"買付先URL5\","+
					"\"買付先URL6\","+
					"\"買付先URL7\","+
					"\"買付先URL8\","+
					"\"買付先URL9\","+
					"\"買付先URL10\","+
					"\"買付先URL11\","+
					"\"買付先URL12\","+
					"\"買付先URL13\","+
					"\"買付先URL14\","+
					"\"買付先URL15\","+
					"\"買付先説明1\","+
					"\"買付先説明2\","+
					"\"買付先説明3\","+
					"\"買付先説明4\","+
					"\"買付先説明5\","+
					"\"買付先説明6\","+
					"\"買付先説明7\","+
					"\"買付先説明8\","+
					"\"買付先説明9\","+
					"\"買付先説明10\","+
					"\"買付先説明11\","+
					"\"買付先説明12\","+
					"\"買付先説明13\","+
					"\"買付先説明14\","+
					"\"買付先説明15\")"+
					" SELECT "+
					"\"商品ID\","+
					"\"商品管理番号\","+
					"'停止', "+ // stop item status
					"\"公開ステータス\","+
					"\"商品名\","+
					"\"ブランド\","+
					"\"ブランド名\","+
					"\"モデル\","+
					"\"カテゴリ\","	+
					"\"シーズン\","+
					"\"テーマ\","+
					"\"単価\","+
					"\"買付可数量\","	+
					"\"購入期限\","+
					"\"参考価格/通常出品価格\","+
					"\"参考価格\","+
					"\"商品コメント\","+
					"\"色サイズ補足\","+
					"\"タグ\","+
					"\"配送方法\","+
					"\"買付エリア\","+
					"\"買付都市\","+
					"\"買付ショップ\","+
					"\"発送エリア\","+
					"\"発送都市\","+
					"\"関税込み\","+
					"\"出品メモ\","+
					"\"商品イメージ1\","+
					"\"商品イメージ2\","+
					"\"商品イメージ3\","+
					"\"商品イメージ4\","+
					"\"商品イメージ5\","+
					"\"商品イメージ6\","+
					"\"商品イメージ7\","+
					"\"商品イメージ8\","+
					"\"商品イメージ9\","+
					"\"商品イメージ10\","+
					"\"商品イメージ11\","+
					"\"商品イメージ12\","+
					"\"商品イメージ13\","+
					"\"商品イメージ14\","+
					"\"商品イメージ15\","+
					"\"商品イメージ16\","+
					"\"商品イメージ17\","+
					"\"商品イメージ18\","+
					"\"商品イメージ19\","+
					"\"商品イメージ20\","+
					"\"ブランド型番1\","+
					"\"ブランド型番2\","+
					"\"ブランド型番3\","+
					"\"ブランド型番4\","+
					"\"ブランド型番5\","+
					"\"ブランド型番6\","+
					"\"ブランド型番7\","+
					"\"ブランド型番8\","+
					"\"ブランド型番9\","+
					"\"ブランド型番10\","+
					"\"ブランド型番識別メモ1\","+
					"\"ブランド型番識別メモ2\","+
					"\"ブランド型番識別メモ3\","+
					"\"ブランド型番識別メモ4\","+
					"\"ブランド型番識別メモ5\","+
					"\"ブランド型番識別メモ6\","+
					"\"ブランド型番識別メモ7\","+
					"\"ブランド型番識別メモ8\","+
					"\"ブランド型番識別メモ9\","+
					"\"ブランド型番識別メモ10\","+
					"\"買付先名1\","+
					"\"買付先名2\","+
					"\"買付先名3\","+
					"\"買付先名4\","+
					"\"買付先名5\","+
					"\"買付先名6\","+
					"\"買付先名7\","+
					"\"買付先名8\","+
					"\"買付先名9\","+
					"\"買付先名10\","	+
					"\"買付先名11\","	+
					"\"買付先名12\","	+
					"\"買付先名13\","	+
					"\"買付先名14\","	+
					"\"買付先名15\","	+
					"\"買付先URL1\","+
					"\"買付先URL2\","+
					"\"買付先URL3\","+
					"\"買付先URL4\","+
					"\"買付先URL5\","+
					"\"買付先URL6\","+
					"\"買付先URL7\","+
					"\"買付先URL8\","+
					"\"買付先URL9\","+
					"\"買付先URL10\","+
					"\"買付先URL11\","+
					"\"買付先URL12\","+
					"\"買付先URL13\","+
					"\"買付先URL14\","+
					"\"買付先URL15\","+
					"\"買付先説明1\","+
					"\"買付先説明2\","+
					"\"買付先説明3\","+
					"\"買付先説明4\","+
					"\"買付先説明5\","+
					"\"買付先説明6\","+
					"\"買付先説明7\","+
					"\"買付先説明8\","+
					"\"買付先説明9\","+
					"\"買付先説明10\","+
					"\"買付先説明11\","+
					"\"買付先説明12\","+
					"\"買付先説明13\","+
					"\"買付先説明14\","+
					"\"買付先説明15\""+
					"FROM BUYMA_ITEMS "+
					"WHERE \"商品管理番号\" in "+
					"("+
					" SELECT ITEM_ID "+
					" FROM ANTONIOLI "+
					" WHERE ITEM_ID IN "+
					" ("+
					"  SELECT DISTINCT ITEM_ID "+
					"  FROM ANTONIOLI "+
					"  WHERE STATUS = "+StagingRecordStatus.DELETED+" "+
					"  AND BUYMA_STATUS = "+StagingBuymaStatus.TO_BE_PROCESSED+" "+
					" )"+
					" GROUP BY ITEM_ID "+
					" HAVING COUNT(DISTINCT STATUS) = 1 "+
					");";			
			if(logger!=null)
				logger.log(Level.INFO, "Create records into table ANTONIOLI_ITEMS_TO_BUYMA to stop items with all size at 0 stock.");
			destinationSqlLiteDB.executeUpdate(sqlStatement);
			*/
			//update the buyma status
			Timestamp c = new Timestamp(System.currentTimeMillis());
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sss");
			sqlStatement = "UPDATE ANTONIOLI "+
					"SET BUYMA_PROCESSED_TIMESTAMP= '"+sdf.format(c)+"' "+
					"WHERE (STATUS="+StagingRecordStatus.DELETED+" "+ 
					"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
			if(logger!=null)
				logger.log(Level.INFO, "Update processed timestap records into table ANTONIOLI with 0 stock.");
			destinationSqlLiteDB.executeUpdate(sqlStatement);
			
			sqlStatement = "UPDATE ANTONIOLI "+
					"SET BUYMA_STATUS= "+StagingBuymaStatus.PROCESSED+" "+
					"WHERE (STATUS="+StagingRecordStatus.DELETED+" "+ 
					"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
			if(logger!=null)
				logger.log(Level.INFO, "Update status record records into table ANTONIOLI with 0 stock as processed.");
			destinationSqlLiteDB.executeUpdate(sqlStatement);
					
			//MANAGE ITEMS TO BE UPDATED
			//changes allowed only for price, price currency, description and images links
			sqlStatement = " "+
					"INSERT INTO ANTONIOLI_ITEMS_TO_BUYMA (" +
					"\"商品ID\","+
					"\"商品管理番号\","+
					"\"コントロール\","+
					"\"公開ステータス\","+
					"\"商品名\","+
					"\"ブランド\","+
					"\"ブランド名\","+
					"\"モデル\","+
					"\"カテゴリ\","	+
					"\"シーズン\","+
					"\"テーマ\","+
					"\"単価\","+
					"\"買付可数量\","	+
					"\"購入期限\","+
					"\"参考価格/通常出品価格\","+
					"\"参考価格\","+
					"\"商品コメント\","+
					"\"色サイズ補足\","+
					"\"タグ\","+
					"\"配送方法\","+
					"\"買付エリア\","+
					"\"買付都市\","+
					"\"買付ショップ\","+
					"\"発送エリア\","+
					"\"発送都市\","+
					"\"関税込み\","+
					"\"出品メモ\","+
					"\"商品イメージ1\","+
					"\"商品イメージ2\","+
					"\"商品イメージ3\","+
					"\"商品イメージ4\","+
					"\"商品イメージ5\","+
					"\"商品イメージ6\","+
					"\"商品イメージ7\","+
					"\"商品イメージ8\","+
					"\"商品イメージ9\","+
					"\"商品イメージ10\","+
					"\"商品イメージ11\","+
					"\"商品イメージ12\","+
					"\"商品イメージ13\","+
					"\"商品イメージ14\","+
					"\"商品イメージ15\","+
					"\"商品イメージ16\","+
					"\"商品イメージ17\","+
					"\"商品イメージ18\","+
					"\"商品イメージ19\","+
					"\"商品イメージ20\","+
					"\"ブランド型番1\","+
					"\"ブランド型番2\","+
					"\"ブランド型番3\","+
					"\"ブランド型番4\","+
					"\"ブランド型番5\","+
					"\"ブランド型番6\","+
					"\"ブランド型番7\","+
					"\"ブランド型番8\","+
					"\"ブランド型番9\","+
					"\"ブランド型番10\","+
					"\"ブランド型番識別メモ1\","+
					"\"ブランド型番識別メモ2\","+
					"\"ブランド型番識別メモ3\","+
					"\"ブランド型番識別メモ4\","+
					"\"ブランド型番識別メモ5\","+
					"\"ブランド型番識別メモ6\","+
					"\"ブランド型番識別メモ7\","+
					"\"ブランド型番識別メモ8\","+
					"\"ブランド型番識別メモ9\","+
					"\"ブランド型番識別メモ10\","+
					"\"買付先名1\","+
					"\"買付先名2\","+
					"\"買付先名3\","+
					"\"買付先名4\","+
					"\"買付先名5\","+
					"\"買付先名6\","+
					"\"買付先名7\","+
					"\"買付先名8\","+
					"\"買付先名9\","+
					"\"買付先名10\","	+
					"\"買付先名11\","	+
					"\"買付先名12\","	+
					"\"買付先名13\","	+
					"\"買付先名14\","	+
					"\"買付先名15\","	+
					"\"買付先URL1\","+
					"\"買付先URL2\","+
					"\"買付先URL3\","+
					"\"買付先URL4\","+
					"\"買付先URL5\","+
					"\"買付先URL6\","+
					"\"買付先URL7\","+
					"\"買付先URL8\","+
					"\"買付先URL9\","+
					"\"買付先URL10\","+
					"\"買付先URL11\","+
					"\"買付先URL12\","+
					"\"買付先URL13\","+
					"\"買付先URL14\","+
					"\"買付先URL15\","+
					"\"買付先説明1\","+
					"\"買付先説明2\","+
					"\"買付先説明3\","+
					"\"買付先説明4\","+
					"\"買付先説明5\","+
					"\"買付先説明6\","+
					"\"買付先説明7\","+
					"\"買付先説明8\","+
					"\"買付先説明9\","+
					"\"買付先説明10\","+
					"\"買付先説明11\","+
					"\"買付先説明12\","+
					"\"買付先説明13\","+
					"\"買付先説明14\","+
					"\"買付先説明15\")"+
					" SELECT "+
					"bi.\"商品ID\","+
					"bi.\"商品管理番号\","+
					"bi.\"コントロール\", "+
					"bi.\"公開ステータス\","+
					"bi.\"商品名\","+
					"bi.\"ブランド\","+
					"bi.\"ブランド名\","+
					"bi.\"モデル\","+
					"bi.\"カテゴリ\","	+
					"bi.\"シーズン\","+
					"bi.\"テーマ\","+
					"bi.\"単価\","+
					"bi.\"買付可数量\","	+
					"bi.\"購入期限\","+
					"bi.\"参考価格/通常出品価格\","+
					"bi.\"参考価格\","+
					"an.ITEM_DESCRIPTION,"+
					"bi.\"色サイズ補足\","+
					"bi.\"タグ\","+
					"bi.\"配送方法\","+
					"bi.\"買付エリア\","+
					"bi.\"買付都市\","+
					"bi.\"買付ショップ\","+
					"bi.\"発送エリア\","+
					"bi.\"発送都市\","+
					"bi.\"関税込み\","+
					"CAST(CAST(substr(CAST((ROUND((CAST(an.ITEM_PRICE as REAL))/1.22,2)-(ROUND((CAST(an.ITEM_PRICE as REAL))/1.22,2)*0.2)) as TEXT),1,instr(CAST((ROUND((CAST(an.ITEM_PRICE as REAL))/1.22,2)-(ROUND((CAST(an.ITEM_PRICE as REAL))/1.22,2)*0.2)) as TEXT),'.')-1) as REAL) +1 as TEXT) || '"+"\n"+"' || an.ITEM_LINK, "+
					"an.ITEM_PICTURE,"+
					"an.ITEM_PICTURE_ALT1,"+
					"an.ITEM_PICTURE_ALT2,"+
					"an.ITEM_PICTURE_ALT3,"+
					"an.ITEM_PICTURE_ALT4,"+
					"bi.\"商品イメージ6\","+
					"bi.\"商品イメージ7\","+
					"bi.\"商品イメージ8\","+
					"bi.\"商品イメージ9\","+
					"bi.\"商品イメージ10\","+
					"bi.\"商品イメージ11\","+
					"bi.\"商品イメージ12\","+
					"bi.\"商品イメージ13\","+
					"bi.\"商品イメージ14\","+
					"bi.\"商品イメージ15\","+
					"bi.\"商品イメージ16\","+
					"bi.\"商品イメージ17\","+
					"bi.\"商品イメージ18\","+
					"bi.\"商品イメージ19\","+
					"bi.\"商品イメージ20\","+
					"bi.\"ブランド型番1\","+
					"bi.\"ブランド型番2\","+
					"bi.\"ブランド型番3\","+
					"bi.\"ブランド型番4\","+
					"bi.\"ブランド型番5\","+
					"bi.\"ブランド型番6\","+
					"bi.\"ブランド型番7\","+
					"bi.\"ブランド型番8\","+
					"bi.\"ブランド型番9\","+
					"bi.\"ブランド型番10\","+
					"bi.\"ブランド型番識別メモ1\","+
					"bi.\"ブランド型番識別メモ2\","+
					"bi.\"ブランド型番識別メモ3\","+
					"bi.\"ブランド型番識別メモ4\","+
					"bi.\"ブランド型番識別メモ5\","+
					"bi.\"ブランド型番識別メモ6\","+
					"bi.\"ブランド型番識別メモ7\","+
					"bi.\"ブランド型番識別メモ8\","+
					"bi.\"ブランド型番識別メモ9\","+
					"bi.\"ブランド型番識別メモ10\","+
					"bi.\"買付先名1\","+
					"bi.\"買付先名2\","+
					"bi.\"買付先名3\","+
					"bi.\"買付先名4\","+
					"bi.\"買付先名5\","+
					"bi.\"買付先名6\","+
					"bi.\"買付先名7\","+
					"bi.\"買付先名8\","+
					"bi.\"買付先名9\","+
					"bi.\"買付先名10\","	+
					"bi.\"買付先名11\","	+
					"bi.\"買付先名12\","	+
					"bi.\"買付先名13\","	+
					"bi.\"買付先名14\","	+
					"bi.\"買付先名15\","	+
					"bi.\"買付先URL1\","+
					"bi.\"買付先URL2\","+
					"bi.\"買付先URL3\","+
					"bi.\"買付先URL4\","+
					"bi.\"買付先URL5\","+
					"bi.\"買付先URL6\","+
					"bi.\"買付先URL7\","+
					"bi.\"買付先URL8\","+
					"bi.\"買付先URL9\","+
					"bi.\"買付先URL10\","+
					"bi.\"買付先URL11\","+
					"bi.\"買付先URL12\","+
					"bi.\"買付先URL13\","+
					"bi.\"買付先URL14\","+
					"bi.\"買付先URL15\","+
					"bi.\"買付先説明1\","+
					"bi.\"買付先説明2\","+
					"bi.\"買付先説明3\","+
					"bi.\"買付先説明4\","+
					"bi.\"買付先説明5\","+
					"bi.\"買付先説明6\","+
					"bi.\"買付先説明7\","+
					"bi.\"買付先説明8\","+
					"bi.\"買付先説明9\","+
					"bi.\"買付先説明10\","+
					"bi.\"買付先説明11\","+
					"bi.\"買付先説明12\","+
					"bi.\"買付先説明13\","+
					"bi.\"買付先説明14\","+
					"bi.\"買付先説明15\" "+
					"FROM BUYMA_ITEMS bi INNER JOIN ANTONIOLI an ON bi.\"商品管理番号\" = an.ITEM_ID "+
					"WHERE an.STATUS = "+StagingRecordStatus.MODIFIED+" "+
					"AND an.BUYMA_STATUS = "+StagingBuymaStatus.TO_BE_PROCESSED+";";
			if(logger!=null)
				logger.log(Level.INFO, "Create records into the table ANTONIOLI_ITEMS_TO_BUYMA that have some changes.");
			destinationSqlLiteDB.executeUpdate(sqlStatement);
			
			//update the buyma status
			c = new Timestamp(System.currentTimeMillis());
			sqlStatement = "UPDATE ANTONIOLI "+
					"SET BUYMA_PROCESSED_TIMESTAMP= '"+sdf.format(c)+"' "+
					"WHERE (STATUS="+StagingRecordStatus.MODIFIED+" "+ 
					"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
			if(logger!=null)
				logger.log(Level.INFO, "Update processed timestap records into table ANTONIOLI with 0 stock as processed.");
			destinationSqlLiteDB.executeUpdate(sqlStatement);
						
			sqlStatement = "UPDATE ANTONIOLI "+
					"SET BUYMA_STATUS= "+StagingBuymaStatus.PROCESSED+" "+
					"WHERE (STATUS="+StagingRecordStatus.MODIFIED+" "+ 
					"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
			
			if(logger!=null)
				logger.log(Level.INFO, "Update records modified into table ANTONIOLI as processed.");
			destinationSqlLiteDB.executeUpdate(sqlStatement);
			
			//MANAGE NEW ITEMS
			sqlStatement = "SELECT * FROM ANTONIOLI "+
							"WHERE STATUS="+StagingRecordStatus.NEW+" "+
							"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+";";
			if(logger!=null)
				logger.log(Level.FINE, "Get records from the table ANTONIOLI marked as new.");
			ResultSet rs = destinationSqlLiteDB.executeSelect(sqlStatement);
			String currentItemId = "";
			String candidateBrandCode = "";
			String candidateBrandDesc = "";
			String candidateBrandDescJP = "";
			String candidateCategory = "";
			String candidateSize = "";
			String candidateSizeDesc = "";
			String candidateColorCode = "1";
			String candidateColorDesc = "WHITE";
			String colorComment = "No Color Found";
			String candidateDesc = "";
			DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE;
			LocalDateTime expireDate =  LocalDateTime.now();
			expireDate = expireDate.plusDays(14);
			while(expireDate.getDayOfWeek() != DayOfWeek.SUNDAY)
				expireDate = expireDate.plusDays(1);
			int sizeRowCount = 1;
			String comment ="";
			String commentText = ""+
					"――――――――――――――――――――――――――――――\n"+
					"★必ず「お取引について」欄をお読み下さいませ。\n"+
					"\n"+
					"★発送はイタリアから、送料込みのお値段です。\n"+
					"\n"+
					"★海外発送の為関税は別途かかります。\n"+
					"BUYMAの規定通り、海外発送の商品については、\n"+
					"関税（消費税・立替手数料など含む）のお支払いは購入者負担となります。\n"+
					"関税は荷物お受け取りの際、配達員にお支払いくださいませ。\n"+
					"\n"+
					"→(こちらで関税支払い手続き後の発送も可能です。\n"+
					"お値段が変わりますのでご連絡ください)\n"+
					"\n"+
					"★ご決済後5日〜10日(営業日)前後のお届けになります。\n"+
					"\n"+
					"★ご注文の際、在庫切れの場合はご注文をお取消しさせていただきます。\n"+
					"　ご了承頂きますようお願いします。\n"+
					"\n"+
					"――――――――――――――――――――――――――――――\n"+
					"――――――――――――――――――――――――――――――\n"+
					"\n"+
					"★こちらの商品はご注文後の買い付け対象商品です。\n"+
					"\n"+
					"★ご購入前にご面倒ですが在庫確認をお願いいたします。\n"+
					"店舗確認して在庫の有無・予想関税額 を折り返しご連絡いたします。\n"+
					"\n"+
					"★商品は正規販売代理店からの買い付けです。\n"+
					"ご購入の際にインボイス(輸入証明書、買い付け店舗、商品の素材等の詳細が記載されています)が発行され、商品と共に納品されます。\n"+
					"100%本物ですので安心してご購入ください。\n"+
					"\n"+
					"★ご利用画面の発色具合や撮影状況により、実際の商品と色味や素材感が異なる場合がございます。あらかじめご了承の上、ご注文ください。\n"+
					"\n"+
					"★配送後の商品紛失、破損はバイヤー側では一切責任を負いかねます。\n"+
					"「あんしんプラス」のご利用をお勧めいたします。商品紛失の際に全額補償がございます。\n"+
					"\n"+
					"「あんしんプラス」→ https://www.buyma.com/contents/safety/anshin.html";
			
			if(logger!=null)
				logger.log(Level.FINE, "Retreive all brands and store in list.");
			sqlStatement = "SELECT * FROM BUYMA_BRAND_ID;";
			ResultSet brandsRs = destinationSqlLiteDB.executeSelect(sqlStatement);
			List<String> brandsCode = new ArrayList<String>();
			List<String> brandsDesc = new ArrayList<String>();
			List<String> brandsDescJP = new ArrayList<String>();
			while(brandsRs.next())
			{
				brandsCode.add(brandsRs.getString("ブランドＩＤ"));
				brandsDesc.add(brandsRs.getString("ブランド名(英語)"));
				brandsDescJP.add(brandsRs.getString("ブランド名(カナ)"));
			}
			brandsRs.close();
			
			if(logger!=null)
				logger.log(Level.FINE, "Iterate on all items marked as new.");
			while (rs.next()) 
			{				
				if(!currentItemId.equals(rs.getString("ITEM_SKU")))
				{
					currentItemId = rs.getString("ITEM_SKU");
					sizeRowCount = 1;
					
					//new item found, check if it is processable
					if(logger!=null)
						logger.log(Level.FINE, "Start to process the item "+rs.getString("ITEM_SKU"));		
					/*
					//check size
					if(rs.getString("ITEM_HIERARCHY2").equals("Size (JEANS)") 
							|| rs.getString("ITEM_HIERARCHY2").equals("Size (Scarpe)")
							|| rs.getString("ITEM_HIERARCHY2").equals("Size (SHIRT NECK)"))
					{
						if(logger!=null)					
							logger.log(Level.WARNING, "Size "+rs.getString("ITEM_HIERARCHY2")+" not covered, the item will be marked as unprocessable.");
						
						sqlStatement = "UPDATE ANTONIOLI "+
								"SET BUYMA_STATUS_DESC= 'Size type "+rs.getString("ITEM_HIERARCHY2")+" not managed' "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+
								"AND ITEM_SKU='"+rs.getString("ITEM_SKU")+"' "+
								"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
						if(logger!=null)
							logger.log(Level.INFO, "Update the record into the table ANTONIOLI as unprocessable.");
						destinationSqlLiteDB.executeUpdate(sqlStatement);
						
						sqlStatement = "UPDATE ANTONIOLI "+
								"SET BUYMA_STATUS= "+StagingBuymaStatus.UNPROCESSABLE+" "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+
								"AND ITEM_SKU='"+rs.getString("ITEM_SKU")+"' "+
								"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
						if(logger!=null)
							logger.log(Level.INFO, "Update the record into the table ANTONIOLI as unprocessable with reason.");
						destinationSqlLiteDB.executeUpdate(sqlStatement);
										
						continue;
					}	
					*/
					//BRAND
					if(logger!=null)
						logger.log(Level.FINE, "Look for the most suitable brand.");
					
					if(rs.getString("ITEM_BRAND").equals("Off-White c/o Virgil Abloh"))
					{
						candidateBrandCode = "5041";
						candidateBrandDesc = "Off-White";
						candidateBrandDescJP = "オフホワイト";
					}
					else if(rs.getString("ITEM_BRAND").equals("Chloé"))
					{
						candidateBrandCode = "85";
						candidateBrandDesc = "Chloe";
						candidateBrandDescJP = "クロエ";
					}
					else if(rs.getString("ITEM_BRAND").contains("Comme Des Garçons"))
					{
						candidateBrandCode = "170";
						candidateBrandDesc = "COMME des GARCONS";
						candidateBrandDescJP = "コムデギャルソン";
					}
					else
					{
						ExtractedResult fuzzyResult = FuzzySearch.extractOne((rs.getString("ITEM_BRAND")).toUpperCase(), brandsDesc);
									
						if(fuzzyResult.getScore() >= 90)
						{
							candidateBrandCode = brandsCode.get(fuzzyResult.getIndex());
							candidateBrandDesc = fuzzyResult.getString();
							candidateBrandDescJP = brandsDescJP.get(fuzzyResult.getIndex());
						}
						else
						{
							if(logger!=null)
								logger.log(Level.WARNING, "No strong brand found as suitable, the item will be marked as unprocessable.");
							
							sqlStatement = "UPDATE ANTONIOLI "+
									"SET BUYMA_STATUS_DESC= 'No strong suitable brand code found' "+
									"WHERE (STATUS="+StagingRecordStatus.NEW+" "+
									"AND ITEM_SKU='"+rs.getString("ITEM_SKU")+"' "+
									"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
							destinationSqlLiteDB.executeUpdate(sqlStatement);
							
							sqlStatement = "UPDATE ANTONIOLI "+
									"SET BUYMA_STATUS= "+StagingBuymaStatus.UNPROCESSABLE+" "+
									"WHERE (STATUS="+StagingRecordStatus.NEW+" "+
									"AND ITEM_SKU='"+rs.getString("ITEM_SKU")+"' "+
									"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
							if(logger!=null)
								logger.log(Level.INFO, "Update the record into the tabel ANTONIOLI as unprocessable.");
							destinationSqlLiteDB.executeUpdate(sqlStatement);
							continue;
						}
					}
					
					comment = candidateBrandDesc + " (" + candidateBrandDescJP + ")" + "\n" + commentText;					
					
					//CATEGORY
					if(logger!=null)
						logger.log(Level.FINE, "Retreive categories and look for the most suitable.");
					
					String g = ((rs.getString("ITEM_GENDER").trim()).equals("W")) ? "WOMEN" : "MEN";
					
					sqlStatement = "SELECT COUNT(*) FROM BUYMA_CATEGORY_ID_FROM_ANTONIOLI "+
							"WHERE ANTONIOLI_GENDER='"+g+"' AND "+
							"ANTONIOLI_CATEGORY_2A='"+rs.getString("ITEM_CATEGORY").toUpperCase()+"' OR "+
							"ANTONIOLI_CATEGORY_2B='"+rs.getString("ITEM_CATEGORY").toUpperCase()+"';";
					
					//get number of categories
					ResultSet categoriesNum = destinationSqlLiteDB.executeSelect(sqlStatement);
					while(categoriesNum.next())
					{
						//if number of categories = 0 than not processable
						if(categoriesNum.getInt(1)==0)
						{	
							if(logger!=null)
								logger.log(Level.WARNING, "No category found, the item will be marked as unprocessable.");
							
							sqlStatement = "UPDATE ANTONIOLI "+
									"SET BUYMA_STATUS_DESC= 'No category code found' "+
									"WHERE (STATUS="+StagingRecordStatus.NEW+" "+
									"AND ITEM_SKU='"+rs.getString("ITEM_SKU")+"' "+
									"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
							destinationSqlLiteDB.executeUpdate(sqlStatement);
							
							sqlStatement = "UPDATE ANTONIOLI "+
									"SET BUYMA_STATUS= "+StagingBuymaStatus.UNPROCESSABLE+" "+
									"WHERE (STATUS="+StagingRecordStatus.NEW+" "+
									"AND ITEM_SKU='"+rs.getString("ITEM_SKU")+"' "+
									"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
							if(logger!=null)
								logger.log(Level.INFO, "Update the record into the table ANTONIOLI as unprocessable.");
							destinationSqlLiteDB.executeUpdate(sqlStatement);
							continue;
						}//if no category found
						else
						{
							ResultSet categories = null;
							if(categoriesNum.getInt(1)==1)
							{
								//found one category
								if(logger!=null)
									logger.log(Level.FINE, "Found one category for the item.");
								sqlStatement = "SELECT * FROM BUYMA_CATEGORY_ID_FROM_ANTONIOLI "+
										"WHERE ANTONIOLI_GENDER='"+g+"' AND "+
										"ANTONIOLI_CATEGORY_2A='"+rs.getString("ITEM_CATEGORY").toUpperCase()+"' OR "+
										"ANTONIOLI_CATEGORY_2B='"+rs.getString("ITEM_CATEGORY").toUpperCase()+"';";
								categories = destinationSqlLiteDB.executeSelect(sqlStatement);
								candidateCategory = categories.getString("カテゴリＩＤ");
							}//if one category found
							else
							{
								//more than one category, look for keywords
								if(logger!=null)
									logger.log(Level.FINE, "Found more than one category for the item. Adjust the research.");
								//special case
								if(rs.getString("ITEM_CATEGORY").contains("WALLETS & CARDHOLDERS"))
								{
									if(logger!=null)
										logger.log(Level.FINE, "Special case category WALLETS & CARDHOLDERS.");
									int postionOfWidth = rs.getString("ITEM_CATEGORY").indexOf("WIDTH");
									int postionOfCM = rs.getString("ITEM_CATEGORY").indexOf("CM", postionOfWidth);
									if(postionOfWidth!=-1 && Integer.parseInt(rs.getString("ITEM_CATEGORY").substring(postionOfWidth+6, postionOfCM-1))>=19)
									{
										if(logger!=null)
											logger.log(Level.FINE, "Found category for the special case WALLETS & CARDHOLDERS.");
										candidateCategory = "3408";
									}									
								}//else special case
								else
								{
									//use keywords to look for the right category
									//first we assign the category with no keywords, the general one
									
									sqlStatement = "SELECT * FROM BUYMA_CATEGORY_ID_FROM_ANTONIOLI "+
											"WHERE ANTONIOLI_GENDER='"+g+"' AND "+
											"(ANTONIOLI_CATEGORY_2A='"+rs.getString("ITEM_CATEGORY").toUpperCase()+"' OR "+
											"ANTONIOLI_CATEGORY_2B='"+rs.getString("ITEM_CATEGORY").toUpperCase()+"') AND "+
											"ANTONIOLI_DESC_2A_KEYWORDS = '' AND ANTONIOLI_DESC_2B_KEYWORDS = '';";
									categories = destinationSqlLiteDB.executeSelect(sqlStatement);
									candidateCategory = categories.getString("カテゴリＩＤ");
									
									//then look for a specific one via keywords
									sqlStatement = "SELECT * FROM BUYMA_CATEGORY_ID_FROM_ANTONIOLI "+
											"WHERE ANTONIOLI_GENDER='"+g+"' AND "+
											"(ANTONIOLI_CATEGORY_2A='"+rs.getString("ITEM_CATEGORY").toUpperCase()+"' OR "+
											"ANTONIOLI_CATEGORY_2B='"+rs.getString("ITEM_CATEGORY").toUpperCase()+"') AND "+
											"(ANTONIOLI_DESC_2A_KEYWORDS <> '' OR ANTONIOLI_DESC_2B_KEYWORDS <> '');";
									categories = destinationSqlLiteDB.executeSelect(sqlStatement);
									while(categories.next())
									{
										if(rs.getString("ITEM_DESCRIPTION").contains(categories.getString("ANTONIOLI_DESC_2A_KEYWORDS")) || 
												rs.getString("ITEM_DESCRIPTION").contains(categories.getString("ANTONIOLI_DESC_2A_KEYWORDS")))
										{	
											if(logger!=null)
												logger.log(Level.FINE, "Found category by keywords.");
											candidateCategory = categories.getString("カテゴリＩＤ");
										}
									}
								}								
							}//else more than one category found		
							if(categories != null)
								categories.close();
						}//else category found
					}//while num categories next
					categoriesNum.close();
					
					if(candidateCategory == "")
					{
						if(logger!=null)
							logger.log(Level.WARNING, "No category found, the item will be marked as unprocessable.");
						sqlStatement = "UPDATE ANTONIOLI "+
								"SET BUYMA_STATUS_DESC= 'No category code found' "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+
								"AND ITEM_SKU='"+rs.getString("ITEM_SKU")+"' "+
								"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
						destinationSqlLiteDB.executeUpdate(sqlStatement);
						
						sqlStatement = "UPDATE ANTONIOLI "+
								"SET BUYMA_STATUS= "+StagingBuymaStatus.UNPROCESSABLE+" "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+
								"AND ITEM_SKU='"+rs.getString("ITEM_SKU")+"' "+
								"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
						if(logger!=null)
							logger.log(Level.INFO, "Update the record into the table ANTONIOLI as unprocessable.");
						destinationSqlLiteDB.executeUpdate(sqlStatement);
						continue;
					}
					
					//COLOR
					candidateColorCode = "1";
					candidateColorDesc = "WHITE";
					colorComment = "No Color Found!";
					if(logger!=null)
						logger.log(Level.FINE, "Retreive a color code id available.");
					candidateDesc = "";
					String[] descriptions = rs.getString("ITEM_DESCRIPTION").split(" - ");
					sqlStatement = "SELECT * FROM BUYMA_COLORS;";
					ResultSet colors = destinationSqlLiteDB.executeSelect(sqlStatement);
					if(descriptions.length > 1)
					{
						while(colors.next())
						{
							if(((descriptions[1].toUpperCase()).trim()).contains(colors.getString("COLOR_DESC")))
							{
								candidateColorCode = colors.getString("COLOR_BYUMA_CD");
								candidateColorDesc = colors.getString("COLOR_DESC");
								colorComment = "";
							}
						}
						colors.close();
					}
					
					//DESCRIPTION
					candidateDesc = "ブランド： "+ candidateBrandDesc + " (" + candidateBrandDescJP + ")\n";
					candidateDesc = candidateDesc + "商品名： " + descriptions[0] + "\n";
					candidateDesc = candidateDesc + "ART. " + rs.getString("ITEM_STYLE") + "\n";
					candidateDesc = candidateDesc + "――――――――――――――――――――――――――――――\n";
					candidateDesc = candidateDesc + "詳細 :　\n";
					for(int i = 1; i<descriptions.length; i++)
					{
						candidateDesc = candidateDesc + descriptions[i] + "\n";
					}
					candidateDesc = candidateDesc + "――――――――――――――――――――――――――――――";
					//create the record in table ANTONIOLI_ITEMS_TO_BUYMA
					sqlStatement = " "+
							"INSERT INTO ANTONIOLI_ITEMS_TO_BUYMA (" +
							"\"商品ID\","+
							"\"商品管理番号\","+
							"\"コントロール\","+
							"\"公開ステータス\","+
							"\"商品名\","+
							"\"ブランド\","+
							"\"ブランド名\","+
							"\"モデル\","+
							"\"カテゴリ\","	+
							"\"シーズン\","+
							"\"テーマ\","+
							"\"単価\","+
							"\"買付可数量\","	+
							"\"購入期限\","+
							"\"参考価格/通常出品価格\","+
							"\"参考価格\","+
							"\"商品コメント\","+
							"\"色サイズ補足\","+
							"\"タグ\","+
							"\"配送方法\","+
							"\"買付エリア\","+
							"\"買付都市\","+
							"\"買付ショップ\","+
							"\"発送エリア\","+
							"\"発送都市\","+
							"\"関税込み\","+
							"\"出品メモ\","+
							"\"商品イメージ1\","+
							"\"商品イメージ2\","+
							"\"商品イメージ3\","+
							"\"商品イメージ4\","+
							"\"商品イメージ5\","+
							"\"商品イメージ6\","+
							"\"商品イメージ7\","+
							"\"商品イメージ8\","+
							"\"商品イメージ9\","+
							"\"商品イメージ10\","+
							"\"商品イメージ11\","+
							"\"商品イメージ12\","+
							"\"商品イメージ13\","+
							"\"商品イメージ14\","+
							"\"商品イメージ15\","+
							"\"商品イメージ16\","+
							"\"商品イメージ17\","+
							"\"商品イメージ18\","+
							"\"商品イメージ19\","+
							"\"商品イメージ20\","+
							"\"ブランド型番1\","+
							"\"ブランド型番2\","+
							"\"ブランド型番3\","+
							"\"ブランド型番4\","+
							"\"ブランド型番5\","+
							"\"ブランド型番6\","+
							"\"ブランド型番7\","+
							"\"ブランド型番8\","+
							"\"ブランド型番9\","+
							"\"ブランド型番10\","+
							"\"ブランド型番識別メモ1\","+
							"\"ブランド型番識別メモ2\","+
							"\"ブランド型番識別メモ3\","+
							"\"ブランド型番識別メモ4\","+
							"\"ブランド型番識別メモ5\","+
							"\"ブランド型番識別メモ6\","+
							"\"ブランド型番識別メモ7\","+
							"\"ブランド型番識別メモ8\","+
							"\"ブランド型番識別メモ9\","+
							"\"ブランド型番識別メモ10\","+
							"\"買付先名1\","+
							"\"買付先名2\","+
							"\"買付先名3\","+
							"\"買付先名4\","+
							"\"買付先名5\","+
							"\"買付先名6\","+
							"\"買付先名7\","+
							"\"買付先名8\","+
							"\"買付先名9\","+
							"\"買付先名10\","	+
							"\"買付先名11\","	+
							"\"買付先名12\","	+
							"\"買付先名13\","	+
							"\"買付先名14\","	+
							"\"買付先名15\","	+
							"\"買付先URL1\","+
							"\"買付先URL2\","+
							"\"買付先URL3\","+
							"\"買付先URL4\","+
							"\"買付先URL5\","+
							"\"買付先URL6\","+
							"\"買付先URL7\","+
							"\"買付先URL8\","+
							"\"買付先URL9\","+
							"\"買付先URL10\","+
							"\"買付先URL11\","+
							"\"買付先URL12\","+
							"\"買付先URL13\","+
							"\"買付先URL14\","+
							"\"買付先URL15\","+
							"\"買付先説明1\","+
							"\"買付先説明2\","+
							"\"買付先説明3\","+
							"\"買付先説明4\","+
							"\"買付先説明5\","+
							"\"買付先説明6\","+
							"\"買付先説明7\","+
							"\"買付先説明8\","+
							"\"買付先説明9\","+
							"\"買付先説明10\","+
							"\"買付先説明11\","+
							"\"買付先説明12\","+
							"\"買付先説明13\","+
							"\"買付先説明14\","+
							"\"買付先説明15\")"+
							" VALUES ("+
							"'',"+
							"'"+rs.getString("ITEM_ID")+"',"+
							"'下書き',"+
							"'',"+
							"'送料込!!★" + rs.getString("ITEM_BRAND") + "★" + rs.getString("ITEM_CATEGORY") +"',"+
							"'"+candidateBrandCode+"',"+
							"'"+candidateBrandDesc+"',"+
							"'',"+ //model
							"'"+candidateCategory+"',"+	   //category
							"'',"+ //season
							"'',"+ //tema
							"'',"+ //price
							"'1'," + //quantity
							"'"+expireDate.format(formatter)+"',"+ //expire date
							"'0',"+
							"''," +
							"'"+comment+"',"+
							"'"+candidateDesc+"',"+
							"''," + //tag
							"'213087'," +
							"'2003004'," + 
							"'000'," +
							"''," +
							"'2003004'," +
							"'000'," + 
							"'0'," +
							"CAST(CAST(substr(CAST((ROUND((CAST("+rs.getString("ITEM_PRICE")+" as REAL))/1.22,2)-(ROUND((CAST("+rs.getString("ITEM_PRICE")+" as REAL))/1.22,2)*0.2)) as TEXT),1,instr(CAST((ROUND((CAST("+rs.getString("ITEM_PRICE")+" as REAL))/1.22,2)-(ROUND((CAST("+rs.getString("ITEM_PRICE")+" as REAL))/1.22,2)*0.2)) as TEXT),'.')-1) as REAL) +1 as TEXT) || "+"'\n"+rs.getString("ITEM_LINK")+"\n"+colorComment+"',"+
							"'"+rs.getString("ITEM_PICTURE")+"',"+ 
							"'"+rs.getString("ITEM_PICTURE_ALT1")+"'," +
							"'"+rs.getString("ITEM_PICTURE_ALT2")+"'," +
							"'"+rs.getString("ITEM_PICTURE_ALT3")+"'," +
							"'"+rs.getString("ITEM_PICTURE_ALT4")+"'," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," + //image 20
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," + //brand 10
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," + //memo 10
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," + //boutique name 15
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," + 
							"''," + //boutique url 15 
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," +
							"''," + 
							"''" + //boutique memo 15 
							");";
					if(logger!=null)
						logger.log(Level.FINE, "Run insert statement into ANTONIOLI_ITEMS_TO_BUYMA.");
					destinationSqlLiteDB.executeUpdate(sqlStatement);
				}//new item - header			
				
				//default code
				candidateSizeDesc = "ONESIZE";
				candidateSize = "0";
				//look for size code
									
				String g = (rs.getString("ITEM_GENDER").equals("W")) ? "WOMEN" : "MEN";
				sqlStatement = ""+
						"SELECT DISTINCT BUYMA_SIZE, BUYMA_SIZE_DISPLAY "+
						"FROM BUYMA_SIZE_ID_FROM_ANTONIOLI "+
						"WHERE upper(ANTONIOLI_SIZE_CHART)=upper('"+rs.getString("ITEM_HIERARCHY2")+"') AND "+
						"ANTONIOLI_SIZE='"+rs.getString("ITEM_SIZE")+"' AND "+
						"ANTONIOLI_GENDER='"+g+"' ";
				if(((rs.getString("ITEM_CATEGORY")).toUpperCase()).matches("BOOTS|FLATS|LACE-UPS|LOAFERS|PUMPS|SANDALS|SNEAKERS"))
					sqlStatement = sqlStatement + 
						"AND ANTONIOLI_CATEGORY='SHOES';";
				else
					sqlStatement = sqlStatement + 
					"AND ANTONIOLI_CATEGORY='NO SHOES';";
				ResultSet size = destinationSqlLiteDB.executeSelect(sqlStatement);
				while(size.next())
				{
					candidateSize = size.getString("BUYMA_SIZE");
					candidateSizeDesc = size.getString("BUYMA_SIZE_DISPLAY");
				}						
				
				//create record in ANTONIOLI_COLOR_SIZES_TO_BUYMA
				sqlStatement = " "+
						"INSERT INTO ANTONIOLI_COLOR_SIZES_TO_BUYMA ("+            	
						"\"商品ID\","+
						"\"商品管理番号\","+
						"\"並び順\","+
						"\"サイズ名称\","+
						"\"サイズ単位\","+
						"\"検索用サイズ\","+
						"\"色名称\","+
						"\"色系統\","+
						"\"在庫ステータス\","+
						"\"手元に在庫あり数量\","+
						"\"着丈\","+
						"\"肩幅\","+
						"\"胸囲\","+
						"\"袖丈\","+
						"\"ウエスト\","+
						"\"ヒップ\","+
						"\"総丈\","+
						"\"幅\","+
						"\"股上\","+
						"\"股下\","+
						"\"もも周り\","+
						"\"すそ周り\","+
						"\"スカート丈\","+
						"\"トップ\","+
						"\"アンダー\","+
						"\"高さ\","+
						"\"ヒール高\","+
						"\"つつ周り\","+
						"\"足幅\","+
						"\"マチ\","+
						"\"持ち手\","+
						"\"ストラップ\","+
						"\"奥行\","+
						"\"縦\","+
						"\"横\","+
						"\"厚み\","+
						"\"長さ\","+
						"\"トップ縦\","+
						"\"トップ横\","+
						"\"円周\","+
						"\"手首周り\","+
						"\"文字盤縦\","+
						"\"文字盤横\","+
						"\"フレーム縦\","+
						"\"フレーム横\","+
						"\"レンズ縦\","+
						"\"レンズ横\","+
						"\"テンプル\","+
						"\"全長\","+
						"\"最大幅\","+
						"\"頭周り\","+
						"\"つば\","+
						"\"直径\")"+
						" VALUES ("+
						"'',"+
						"'"+rs.getString("ITEM_ID")+"',"+
						"'"+Integer.toString(sizeRowCount)+"',"+ //size order code
						"'"+candidateSizeDesc+"',"+  //size desc from antonioli web site
						"'',"+  //field null
						"'"+candidateSize+"',"+  //size search key word        
						"'"+candidateColorDesc+"',"+  //color name (temporary white)
						"'"+candidateColorCode+"',"+ //color code (temporary white = 1)
						"'1',"+ //stock status
						"'',"+ //stock quanity
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"'',"+
						"''"+
						");";
				if(logger!=null)
					logger.log(Level.FINE, "Run insert statement into ANTONIOLI_COLOR_SIZES_TO_BUYMA.");
				destinationSqlLiteDB.executeUpdate(sqlStatement);
				
				if(logger!=null)
					logger.log(Level.FINE, "End to process the item "+rs.getString("ITEM_SKU"));
				
				sizeRowCount++;
			}//while
			
			//update items status as processed
			c = new Timestamp(System.currentTimeMillis());
			sqlStatement = "UPDATE ANTONIOLI "+
					"SET BUYMA_PROCESSED_TIMESTAMP= '"+sdf.format(c)+"' "+
					"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
					"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
			if(logger!=null)
				logger.log(Level.INFO, "Update processed timestap records into table ANTONIOLI new as processed.");
			destinationSqlLiteDB.executeUpdate(sqlStatement);
			
			sqlStatement = "UPDATE ANTONIOLI "+
					"SET BUYMA_STATUS= "+StagingBuymaStatus.PROCESSED+" "+
					"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
					"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
			if(logger!=null)
				logger.log(Level.INFO, "Update new records into table ANTONIOLI as processed.");
			destinationSqlLiteDB.executeUpdate(sqlStatement);
			
			destinationSqlLiteDB.closeDBConnection();
			
			if(logger!=null)
				logger.log(Level.FINE, "End loadBuyMaTableFromAntonioli method.");
		}
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
		}
		finally
		{
			if(destinationSqlLiteDB != null && destinationSqlLiteDB.isConnetionOpen())
				destinationSqlLiteDB.closeDBConnection();
		}		
	}//loadBuyMaTable	
}//class

