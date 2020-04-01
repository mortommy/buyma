package com.akabana.buyma;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class BuyMa {

	public static void main(String[] args) 
	{
		String log_file_path = "";
	    String log_file_name = "";
	    int log_file_max_size_MB = 5;
	    String log_level = "";
	    String source_to_process = "";
	    
	    String origin_csv_items_file = "";
	    String origin_csv_color_sizes_file = "";
	    /*
	    String buyma_web_site = "";
	    String buyma_login_page = "";
	    String buyma_username = "";
	    String buyma_password = "";
	    */
	   	    
	    BuyMaProperties bmp = null;	   
	    ManageBuyMa mbm = null;
	    	    	
	    //configuration setting load
		try
		{
			bmp = new BuyMaProperties();		
			bmp.loadProperties();
			log_file_path = bmp.getLogFilePath();
	    	log_file_name = bmp.getLogFileName();
	    	log_file_max_size_MB = Integer.parseInt(bmp.getLogFileMaxSizeMB());
	    	log_level = bmp.getLogFileLevel();
	    	source_to_process = bmp.getSourceToProcess();
	    	
	    	/*
	    	buyma_web_site = bmp.getBuyMaWebSite();
	    	buyma_login_page = bmp.getBuyMaLoginPage();
	    	buyma_username = bmp.getBuyMaUsername();
	    	buyma_password = bmp.getBuyMaPassword();
	    	*/
		}
		catch (Exception e1)
		{
			System.err.println("Error during the properties loading. "+e1.getMessage());
	    	System.exit(1);
		}	
		
		//log file creation
	    Logger logger = null;
	    Handler fileHandler = null;
	    try
	    {			
	    	logger = Logger.getLogger(BuyMa.class.getName());
			logger.setLevel(Level.parse(log_level));
		    fileHandler = new FileHandler(log_file_path+log_file_name,log_file_max_size_MB*1024*1024,5);
		    fileHandler.setFormatter(new LoggerFormatter());		    
		    logger.addHandler(fileHandler);
	    }
	    catch (Exception e2) 
		{
	    	System.err.println("Error during the file log creation. "+e2.getMessage());
	    	System.exit(1);
		}	    
		
	    //start to work with data
		logger.log(Level.INFO, "Start the data manipulation.");
		try
		{
			if(source_to_process.contains("antonioli"))
			{
				ManageAntonioli msa = null;
				
				//logger.log(Level.INFO, "Create the object to work with the Antonioli staging data.");
				//msa = new ManageAntonioli(bmp.getAntonioliSourceDb(), bmp.getBuyMaDb(), logger);
				//logger.log(Level.INFO, "Move data from Antonioli download DB to the related Antonioli staging table of DB Buyma .");
				//msa.loadStagingFromDB();
				//logger.log(Level.INFO, "Move data from Antonioli staging table to consolidated Antonioli table of DB Buyma .");
				//msa.loadConsolidatedTable();				
				
				
				//logger.log(Level.INFO, "Create the object to work with Antonioli consolidated data.");
				//7mbm = new ManageBuyMa(bmp.getBuyMaDb(), logger);
								
				//logger.log(Level.INFO, "Load current data available in buyma from csv files.");
				//mbm.loadCurrentBuyMaFromCSV(bmp.getOriginItemsCsvFile(), bmp.getOriginColorSizesCsvFile(), ',', true, true);
				
				//logger.log(Level.INFO, "Load current data from ANTONIOLI to the table used to create the csv files.");
				//msa.loadBuymaTable();
				
				//logger.log(Level.INFO, "Collect all data from tabels in oredr to create the csv files.");
				//mbm.loadFinalCSVExportTable();
				//mbm.generateBuyMaCsv(bmp.getDestinationItemsCsvFile(), bmp.getDestinationColorSizesCsvFile(), true);
				//logger.log(Level.INFO, "Generate zip with the csv files.");
				//mbm.generateBuyMaZipCsv(bmp.getDestinationZipFile(), true);
			}
			/*
			BuyMaWeb bmw = new BuyMaWeb(buyma_web_site,logger);
			HtmlPage page = bmw.BuyMaLogin2(buyma_login_page, buyma_username, buyma_password);
			*/
		}
		catch (Exception e1)
		{
			System.err.println("Error: "+e1.getMessage());
	    	System.exit(1);
		}
	}
}//class
