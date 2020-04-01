package com.akabana.buyma;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class BuyMaProperties 
{
	protected String log_file_path = "";
	protected String log_file_name="";
	protected String log_file_max_size_MB;
	protected String log_level = "";
	
	protected String source_to_process = "";
	
	protected String antonioliSourceDb = "";	
	
	protected String origin_items_csv = "";
	protected String origin_color_sizes_csv = "";
	protected String destination_items_csv = "";
	protected String destination_color_sizes_csv = "";
	protected String destination_zip_file = "";
	protected String buyma_DB = "";	
	protected String buyma_web_site = "";
	protected String buyma_login_page = "";
	protected String buyma_username = "";
	protected String buyma_password = "";
	
	protected Properties prop;
	protected FileInputStream inputStream;
	protected String propFileName = "resources/config.properties";
	
	public BuyMaProperties() throws Exception
	{
		try
		{
			prop = new Properties();	
			inputStream = new FileInputStream(propFileName);			
		}
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
		}
	}
	
	public void loadProperties() throws IOException
	{
		if (inputStream != null) 
		{
			prop.load(inputStream);
		} 
		else 
		{
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		}
		
		this.log_file_path = prop.getProperty("log_file_path");
		this.log_file_name = prop.getProperty("log_file_name");
		this.log_file_max_size_MB = prop.getProperty("log_file_max_size_MB");
		this.log_level = prop.getProperty("log_level");
		
		this.source_to_process = prop.getProperty("source_to_process");
		
		this.antonioliSourceDb = prop.getProperty("antonioli_source_DB");
		this.buyma_DB = prop.getProperty("buyma_DB");
		
		this.origin_items_csv = prop.getProperty("origin_items_csv");
		this.origin_color_sizes_csv = prop.getProperty("origin_color_sizes_csv");
		
		this.destination_items_csv = prop.getProperty("destination_items_csv");
		this.destination_color_sizes_csv = prop.getProperty("destination_color_size_csv");
		this.destination_zip_file = prop.getProperty("destination_zip_file");
		
		this.buyma_web_site  = prop.getProperty("BuyMa_Web_Site");
		this.buyma_login_page  = prop.getProperty("BuyMa_Login_Page");
		this.buyma_username  = prop.getProperty("BuyMa_Login_Username");
		this.buyma_password  = prop.getProperty("BuyMa_Login_Password");
	}//loadProperties
	
	
	public String getLogFilePath()
	{
		return this.log_file_path;
	}
	
	public String getLogFileName()
	{
		return this.log_file_name;
	}
	
	public String getLogFileMaxSizeMB()
	{
		return this.log_file_max_size_MB;
	}
	
	public String getLogFileLevel()
	{
		return this.log_level;
	}
	
	public String getSourceToProcess()
	{
		return this.source_to_process;
	}
	
	public String getAntonioliSourceDb()
	{
		return this.antonioliSourceDb;
	}
	
	public String getBuyMaDb()
	{
		return this.buyma_DB;
	}
	
	public String getOriginItemsCsvFile()
	{
		return this.origin_items_csv;
	}
	
	public String getOriginColorSizesCsvFile()
	{
		return this.origin_color_sizes_csv;
	}
	
	public String getDestinationItemsCsvFile()
	{
		return this.destination_items_csv;
	}
	
	public String getDestinationZipFile()
	{	
		return this.destination_zip_file;
	}
	
	public String getDestinationColorSizesCsvFile()
	{
		return this.destination_color_sizes_csv;
	}
	
	public String getBuyMaWebSite()
	{
		return this.buyma_web_site;
	}
	
	public String getBuyMaLoginPage()
	{
		return this.buyma_login_page;
	}
	
	public String getBuyMaUsername()
	{
		return this.buyma_username;
	}
	
	public String getBuyMaPassword()
	{
		return this.buyma_password;
	}
	
}//class
