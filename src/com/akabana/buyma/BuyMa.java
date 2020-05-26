package com.akabana.buyma;

import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BuyMa {

	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{
		String log_file_path = "";
	    String log_file_name = "";
	    int log_file_max_size_MB = 5;
	    String log_level = "";
	    String source_to_process = "";
	    
	    String origin_csv_items_file = "";
	    String origin_csv_color_sizes_file = "";
	    
	    String buyma_web_site = "";
	    String buyma_login_page = "";
	    String buyma_username = "";
	    String buyma_password = "";	    
	   	    
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
	    	
	    	buyma_web_site = bmp.getBuyMaWebSite();
	    	buyma_login_page = bmp.getBuyMaLoginPage();
	    	buyma_username = bmp.getBuyMaUsername();
	    	buyma_password = bmp.getBuyMaPassword();
	    	
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
		    logger.log(Level.INFO, "Log file created..");
	    }
	    catch (Exception e2) 
		{
	    	System.err.println("Error during the file log creation. "+e2.getMessage());
	    	System.exit(1);
		}	    
	    
	    try
	    {
		    //download first the current active catalog from buyma
	    	/*
		    logger.log(Level.INFO, "Download first the current active catalog from buyma.");
		    BuyMaWeb bmw = new BuyMaWeb(buyma_web_site, bmp.getChromeDriverPathName(), bmp.getChromeUserPath(), logger);
		    //set the default download folder in the browser
			bmw.setDownloadFilesPath(bmp.getOriginZipFiles());
			bmw.BuyMaLogin(buyma_login_page, buyma_username, buyma_password);
			bmw.downloadAllBuyMaCatalog(bmp.getBuyMaActiveItemsPage(), bmp.getBuyMaDownUpLoadPage());
			Thread.sleep(10000);
			bmw.closeBrowser();
			*/		
	    }
	    catch (Exception e3) 
		{
	    	//no problem if the catalog could not be downloaded
	    	logger.log(Level.WARNING, "Something went wrong during the active catalog download. "+e3.getMessage());
		}
	    
	    try
	    {
	    	
	    	logger.log(Level.INFO, "Create the object to work with buyma data.");
			mbm = new ManageBuyMa(bmp.getBuyMaDb(), logger);
			
//			logger.log(Level.INFO, "Load active items catalog from downloaded zip file, if exists, to Buyma DB.");
//			mbm.loadCurrentBuyMafromZip(bmp.getOriginZipFiles(), ',', true, true, true);
			
	    }
	    catch (Exception e4) 
		{
	    	//the error here can generate a corrupted active items catalog in DB
	    	logger.log(Level.SEVERE, "Something went wrong during the active catalog upload. "+e4.getMessage());
	    	System.exit(1);
		}
	    
	    //start to work with data
		logger.log(Level.INFO, "Start the data import and manipulation.");
		try
		{
			
			if(source_to_process.contains("antonioli"))
			{
				
				logger.log(Level.INFO, "Create the object to work with the Antonioli data.");
				ManageAntonioli msa = new ManageAntonioli(bmp.getAntonioliSourceDb(), bmp.getBuyMaDb(), logger);
				
//				logger.log(Level.INFO,"Move data from Antonioli download DB to the related Antonioli staging table of DB Buyma ."); 
//				msa.loadStagingFromDB();
				  
//				logger.log(Level.INFO, "Move data from Antonioli staging table to consolidated Antonioli table of DB Buyma .");
//				msa.loadConsolidatedTable(Integer.parseInt(bmp.getDataLoadMode()));
				  
//				logger.log(Level.INFO, "Load current data from ANTONIOLI to the table used to create the csv files.");
//				msa.loadBuymaTable();
				  
//				logger.log(Level.INFO, "Collect all data from tables in order to create the csv files.");
//				mbm.loadFinalCSVExportTable();
				 
				
				//logger.log(Level.INFO, "Generate zip with the csv files.");
				//List<String> files = mbm.generateBuyMaCsvZipByBrand(bmp.getDestinationZipFile(), true);
				//logger.log(Level.INFO, "Create the object to work with Antonioli Web.");
				//BuyMaWeb bmw = new BuyMaWeb(buyma_web_site, bmp.getChromeDriverPathName(), bmp.getChromeUserPath(), logger);
				//logger.log(Level.INFO, "Call method to log in.");
				//bmw.BuyMaLogin(buyma_login_page, buyma_username, buyma_password);
				//for(int i = 0; i < files.size(); i++)
				//{
				//	bmw.uploadZipToBuyMa(bmp.getBuyMaDownUpLoadPage(), files.get(i));
				//	Thread.sleep(3000);
				//}
				//bmw.closeBrowser();
				
				logger.log(Level.INFO, "Generate zip with the csv files.");
				String rfile = mbm.generateBuyMaCsvZipAll(bmp.getDestinationZipFile(), true);
				logger.log(Level.INFO, "Create the object to work with Antonioli Web.");
				BuyMaWeb bmw = new BuyMaWeb(buyma_web_site, bmp.getChromeDriverPathName(), bmp.getChromeUserPath(), logger);
				logger.log(Level.INFO, "Call method to log in.");
				bmw.BuyMaLogin(buyma_login_page, buyma_username, buyma_password);
				bmw.uploadZipToBuyMa(bmp.getBuyMaDownUpLoadPage(), rfile);
				Thread.sleep(3000);
				bmw.closeBrowser();
				
		}					
			
			//only for MANUAL UPLOAD
			/*
			BuyMaWeb bmw = new BuyMaWeb(buyma_web_site, bmp.getChromeDriverPathName(), bmp.getChromeUserPath(), logger);
			bmw.BuyMaLogin(buyma_login_page, buyma_username, buyma_password);
			bmw.uploadZipToBuyMa(bmp.getBuyMaDownUpLoadPage(), "C:\\Users\\tmorello\\WebScraperWorkspace\\outbound\\byuma_20200517-210505.zip");
			bmw.closeBrowser();
			*/
		}
		catch (Exception e5)
		{
			System.err.println("Error: "+e5.getMessage());
	    	System.exit(1);
		}
	}
}//class
