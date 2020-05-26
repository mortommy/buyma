package com.akabana.buyma;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import com.akabana.db_utils.*;

public class ManageBuyMa {
	
	protected String pathDb;
	protected DBMS_DB sqlLiteDB = null;
	protected Logger logger = null;
	
	/**
	 * Constructor
	 * @param buymaDb where to work with data. Required tables (ANTONIOLI_ITEMS, BUYMA_ITEMS, BUYMA_COLOR_SIZES)
	 */
	public ManageBuyMa(String buymaDb)
	{
		this.pathDb = buymaDb;
		sqlLiteDB = new SQLLite_DB(this.pathDb);
	}	
	
	/**
	 * Constructor
	 * @param buymaDb where to work with data. Required tables (ANTONIOLI_ITEMS, BUYMA_ITEMS, BUYMA_COLOR_SIZES)
	 * @param logger
	 */
	public ManageBuyMa(String buymaDb, Logger logger)
	{
		this(buymaDb);
		this.logger = logger;
	}
	
	/***
	 * Saves a zip file with two CSV files with data coming from tables BUYMA_ITEMS and BUYMA_COLOR_SIZES
	 * @param destinationPathName  path and name of zip file
	 * @param appendTimeStamp if true appends timestamp to the file name
	 * @throws Exception
	 */
	public String generateBuyMaCsvZipAll(String destinationPathName, boolean appendTimeStamp) throws Exception
	{
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start genrateBuyMaCsvZipAll method.");
			
			//connect to db
			if(logger!=null)
				logger.log(Level.INFO, "Open the db: "+sqlLiteDB.getDbFile());
			sqlLiteDB.connectToDB();
			
			//check if exists source tables
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_ITEMS_EXPORT exists");
			if(!sqlLiteDB.tableExists("BUYMA_ITEMS_EXPORT"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_ITEMS_EXPORT not exist");
				throw new Exception("Source table BUYMA_ITEMS_EXPORT not exists in DB "+sqlLiteDB.getDbFile());
			}
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_COLOR_SIZES_EXPORT exists");
			if(!sqlLiteDB.tableExists("BUYMA_COLOR_SIZES_EXPORT"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_COLOR_SIZES_EXPORT not exist");
				throw new Exception("Source table BUYMA_COLOR_SIZES_EXPORT not exists in DB "+sqlLiteDB.getDbFile());
			}
			
			String sqlStatement = "SELECT * FROM BUYMA_ITEMS_EXPORT;";
			if(logger!=null)
				logger.log(Level.FINE, "Get records from BUYMA_ITEMS_EXPORT table.");
			ResultSet rsItems = sqlLiteDB.executeSelect(sqlStatement);
			
			sqlStatement = "SELECT * FROM BUYMA_COLOR_SIZES_EXPORT;";
			if(logger!=null)
				logger.log(Level.FINE, "Get records from BUYMA_COLOR_SIZES_EXPORT table.");
			ResultSet rsColorSizes = sqlLiteDB.executeSelect(sqlStatement);
			
			if(logger!=null)
				logger.log(Level.FINE, "Call method generateBuyMaZipCsv.");
			String generatedFileName = generateBuyMaZipCsv(destinationPathName, appendTimeStamp, rsItems, rsColorSizes);
			
			sqlLiteDB.closeDBConnection();
			
			if(logger!=null)
				logger.log(Level.FINE, "End genrateBuyMaCsvZipAll method.");		
			
			return generatedFileName;
		}
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
		}
		finally 
    	{
    		if(sqlLiteDB.isConnetionOpen())
    		{
    			sqlLiteDB.closeDBConnection();
    		}
        }
	}//genrateBuyMaCsvZipAll
	
	/***
	 * Saves a zip files with two CSV files with data coming from tables BUYMA_ITEMS and BUYMA_COLOR_SIZES
	 * for each brand found in BUYMA_ITEMS
	 * @param destinationPathName  path and name of zip file
	 * @param appendTimeStamp if true appends timestamp to the file name
	 * @throws Exception
	 */
	public List<String> generateBuyMaCsvZipByBrand(String destinationPathName, boolean appendTimeStamp) throws Exception
	{
		List<String> generatedFiles = new ArrayList<String>();
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start genrateBuyMaCsvZipByBrand method.");
			
			//connect to db
			if(logger!=null)
				logger.log(Level.INFO, "Open the db: "+sqlLiteDB.getDbFile());
			sqlLiteDB.connectToDB();
			
			//check if exists source tables
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_ITEMS_EXPORT exists");
			if(!sqlLiteDB.tableExists("BUYMA_ITEMS_EXPORT"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_ITEMS_EXPORT not exist");
				throw new Exception("Source table BUYMA_ITEMS_EXPORT not exists in DB "+sqlLiteDB.getDbFile());
			}
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_COLOR_SIZES_EXPORT exists");
			if(!sqlLiteDB.tableExists("BUYMA_COLOR_SIZES_EXPORT"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_COLOR_SIZES_EXPORT not exist");
				throw new Exception("Source table BUYMA_COLOR_SIZES_EXPORT not exists in DB "+sqlLiteDB.getDbFile());
			}			
			
			String sqlStatement = "SELECT DISTINCT ブランド FROM BUYMA_ITEMS_EXPORT;";
			if(logger!=null)
				logger.log(Level.FINE, "Get the list of Brands in ANTONIOLI_ITEMS_TO_BUYMA.");
			ResultSet rs = sqlLiteDB.executeSelect(sqlStatement);
			ResultSet rsItems = null;
			ResultSet rsColorsize = null;
			
			while(rs.next())
			{
				if(logger!=null)
					logger.log(Level.FINE, "Get the recorded set by brand: "+rs.getString("ブランド"));
				
				sqlStatement = "SELECT * FROM BUYMA_ITEMS_EXPORT WHERE ブランド='"+rs.getString("ブランド")+"';";
				if(logger!=null)
					logger.log(Level.FINE, "Get records from BUYMA_ITEMS_EXPORT table for brand: "+rs.getString("ブランド"));
				rsItems = sqlLiteDB.executeSelect(sqlStatement);
				
				sqlStatement = "SELECT a.* FROM BUYMA_COLOR_SIZES_EXPORT a join BUYMA_ITEMS_EXPORT b " + 
						" ON a.商品管理番号 = b.商品管理番号 " + 
						"WHERE b.ブランド ='"+rs.getString("ブランド")+"';";				
				if(logger!=null)
					logger.log(Level.FINE, "Get records from BUYMA_COLOR_SIZES_EXPORT table for brand: "+rs.getString("ブランド"));
				rsColorsize = sqlLiteDB.executeSelect(sqlStatement);
				
				if(logger!=null)
					logger.log(Level.FINE, "Call method generateBuyMaZipCsv.");
				String generatedFile = generateBuyMaZipCsv(destinationPathName, appendTimeStamp, rsItems, rsColorsize);
				Thread.sleep(1000);
				generatedFiles.add(generatedFile);
			}		
			
			sqlLiteDB.closeDBConnection();
			
			if(logger!=null)
				logger.log(Level.FINE, "End genrateBuyMaCsvZipByBrand method.");
			
			return generatedFiles;
		}
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
		}
		finally 
    	{
    		if(sqlLiteDB.isConnetionOpen())
    		{
    			sqlLiteDB.closeDBConnection();
    		}
        }
	}//genrateBuyMaCsvZipAll
	
	protected String generateBuyMaZipCsv(String destinationPathName, boolean appendTimeStamp, ResultSet rsItems, ResultSet rsColorsizes) throws Exception
	{
		ZipOutputStream zos = null;
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start generateBuyMaZipCsv method.");
			
			File fileDest = new File(destinationPathName);
			String path = fileDest.getParent() + "\\";
			
			if(logger!=null)
				logger.log(Level.INFO, "Invoke method to generate csv files.");
			generateBuyMaCsv(path+"items.csv", path+"colorsizes.csv",false, rsItems, rsColorsizes);
			
			if(appendTimeStamp)
			{
				if(logger!=null)
					logger.log(Level.FINE, "Add timestamp to file name.");
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
				LocalDateTime date =  LocalDateTime.now();
				int extensionIndex = destinationPathName.indexOf(".zip");
				if(extensionIndex!=-1)
					destinationPathName = destinationPathName.substring(0,extensionIndex)+"_"+date.format(formatter)+".zip";
				else
					destinationPathName  = destinationPathName+"_"+date.format(formatter)+".zip"; 
			}
			
			if(logger!=null)
				logger.log(Level.INFO, "Create file: "+destinationPathName);
			
            FileOutputStream fos = new FileOutputStream(destinationPathName);
            zos = new ZipOutputStream(fos);
            
            if(logger!=null)
				logger.log(Level.INFO, "Add file items "+path+"items.csv to the zip.");
            File srcFile = new File(path+"items.csv");
            FileInputStream fis = new FileInputStream(srcFile);
            zos.putNextEntry(new ZipEntry(srcFile.getName()));
            
            byte[] bytes = Files.readAllBytes(Paths.get(path+"items.csv"));
            zos.write(bytes, 0, bytes.length);            
            zos.closeEntry();
            fis.close();
            
            if(logger!=null)
				logger.log(Level.INFO, "Delete csv file: "+path+"items.csv"); 
            srcFile.delete();            
			
            
            if(logger!=null)
				logger.log(Level.INFO, "Add file colorsizes "+path+"colorsizes.csv to the zip.");
            srcFile = new File(path+"colorsizes.csv");
            fis = new FileInputStream(srcFile);
            zos.putNextEntry(new ZipEntry(srcFile.getName()));
            
            bytes = Files.readAllBytes(Paths.get(path+"colorsizes.csv"));
            zos.write(bytes, 0, bytes.length);            
            zos.closeEntry();
            fis.close();
            
            zos.close();
            
            if(logger!=null)
				logger.log(Level.INFO, "Delete csv file: "+path+"colorsizes.csv"); 
            srcFile.delete();            
			
			if(logger!=null)
				logger.log(Level.FINE, "End generateBuyMaZipCsv method.");
			
			return destinationPathName;
			
		}
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
		}
		finally 
    	{
    		if(zos != null)
    		{
    			zos.close();;
    		}
        }
	}//generateBuyMaZipCsv

	/***
	 * Saves a CSV files with data coming from tables BUYMA_ITEMS and BUYMA_COLOR_SIZES
	 * @param destinationPathNameItems path and name of csv file items
	 * @param destinationPathNameColorSizes path and name of csv file colorsizes
	 * @param appendTimeStamp if true appends timestamp to the file name
	 * @throws Exception 
	 */
	public void generateBuyMaCsvAll(String destinationPathNameItems, String destinationPathNameColorSizes, boolean appendTimeStamp) throws Exception
	{
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start generateBuyMaCsvAll method.");
			
			//connect to db
			if(logger!=null)
				logger.log(Level.INFO, "Open the db: "+sqlLiteDB.getDbFile());
			sqlLiteDB.connectToDB();
			
			//check if exists source tables
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_ITEMS_EXPORT exists");
			if(!sqlLiteDB.tableExists("BUYMA_ITEMS_EXPORT"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_ITEMS_EXPORT not exist");
				throw new Exception("Source table BUYMA_ITEMS_EXPORT not exists in DB "+sqlLiteDB.getDbFile());
			}
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_COLOR_SIZES_EXPORT exists");
			if(!sqlLiteDB.tableExists("BUYMA_COLOR_SIZES_EXPORT"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_COLOR_SIZES_EXPORT not exist");
				throw new Exception("Source table BUYMA_COLOR_SIZES_EXPORT not exists in DB "+sqlLiteDB.getDbFile());
			}
			
			String sqlStatement = "SELECT * FROM BUYMA_ITEMS_EXPORT;";
			if(logger!=null)
				logger.log(Level.FINE, "Get records from BUYMA_ITEMS_EXPORT table.");
			ResultSet rsItems = sqlLiteDB.executeSelect(sqlStatement);
			
			sqlStatement = "SELECT * FROM BUYMA_COLOR_SIZES__EXPORT;";
			if(logger!=null)
				logger.log(Level.FINE, "Get records from BUYMA_COLOR_SIZES_EXPORT table.");
			ResultSet rsColorSizes = sqlLiteDB.executeSelect(sqlStatement);
			
			if(logger!=null)
				logger.log(Level.FINE, "Call method generateBuyMaCsv.");
			generateBuyMaCsv(destinationPathNameItems, destinationPathNameColorSizes, appendTimeStamp, rsItems, rsColorSizes);
		
			if(logger!=null)
				logger.log(Level.FINE, "End generateBuyMaCsvAll method.");
		}	
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
		}
		finally 
    	{
    		if(sqlLiteDB.isConnetionOpen())
    		{
    			sqlLiteDB.closeDBConnection();
    		}
        }
	}//generateBuyMaCsv
	
	
	protected void generateBuyMaCsv(String destinationPathNameItems, String destinationPathNameColorSizes, boolean appendTimeStamp, ResultSet rsItems, ResultSet rsColorsizes) throws Exception
	{
		CSVWriter writer = null;
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start generateBuyMaCsv method."); 
			
			//complete filename if timestamp is required
			if(appendTimeStamp)
			{	
				if(logger!=null)
					logger.log(Level.FINE, "Add timestamp to file names.");
				
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
				LocalDateTime date =  LocalDateTime.now();
				int extensionIndex = destinationPathNameItems.indexOf(".csv");
				if(extensionIndex!=-1)
					destinationPathNameItems = destinationPathNameItems.substring(0,extensionIndex)+"_"+date.format(formatter)+".csv";
				else
					destinationPathNameItems  = destinationPathNameItems+"_"+date.format(formatter)+".csv"; 
				
				extensionIndex = destinationPathNameColorSizes.indexOf(".csv");
				if(extensionIndex!=-1)
					destinationPathNameColorSizes = destinationPathNameColorSizes.substring(0,extensionIndex)+"_"+date.format(formatter)+".csv";
				else
					destinationPathNameColorSizes  = destinationPathNameColorSizes+"_"+date.format(formatter)+".csv"; 
			}
			
			if(logger!=null)
				logger.log(Level.INFO, "Write items record to csv file: "+destinationPathNameItems);
			writer = new CSVWriter(new FileWriter(destinationPathNameItems));						
			writer.writeAll(rsItems, true, true, true);
			writer.close();
			
			if(logger!=null)
				logger.log(Level.INFO, "Write items record to csv file: "+destinationPathNameColorSizes);
			writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(destinationPathNameColorSizes), "utf-8"));
			writer.writeAll(rsColorsizes, true, true, true);
			writer.close();
			
			if(logger!=null)
				logger.log(Level.FINE, "End generateBuyMaCsv method.");
		}
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
		}
	}//generateBuyMaCsv
	
	/***
	 * Collect records from all tables containing byuma record in one final table BUYMA_ITEMS_EXPORT
	 * and BUYMA_COLOR_SIZES_EXPORT
	 * @throws Exception 
	 */
	public void loadFinalCSVExportTable() throws Exception
	{
		
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start loadFinalCSVExportTable method.");
			
			if(logger!=null)
				logger.log(Level.INFO, "Open the db: "+sqlLiteDB.getDbFile());
			sqlLiteDB.connectToDB();
			
			//check if destinations tables exists
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_ITEMS_EXPORT exists");
			if(!sqlLiteDB.tableExists("BUYMA_ITEMS_EXPORT"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_ITEMS_EXPORT not exist");
				throw new Exception("Source table BUYMA_ITEMS_EXPORT not exists in DB "+sqlLiteDB.getDbFile());
			}
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_COLOR_SIZES_EXPORT exists");
			if(!sqlLiteDB.tableExists("BUYMA_COLOR_SIZES_EXPORT"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_COLOR_SIZES_EXPORT not exist");
				throw new Exception("Source table BUYMA_COLOR_SIZES_EXPORT not exists in DB "+sqlLiteDB.getDbFile());
			}
			
			//********** ANTONIOLI START CHECK TBALES **********
			if(logger!=null)
				logger.log(Level.FINE, "Check if table ANTONIOLI_ITEMS_TO_BUYMA exists");
			if(!sqlLiteDB.tableExists("ANTONIOLI_ITEMS_TO_BUYMA"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table ANTONIOLI_ITEMS_TO_BUYMA not exist");
				throw new Exception("Source table ANTONIOLI_ITEMS_TO_BUYMA not exists in DB "+sqlLiteDB.getDbFile());
			}
			if(logger!=null)
				logger.log(Level.FINE, "Check if table ANTONIOLI_COLOR_SIZES_TO_BUYMA exists");
			if(!sqlLiteDB.tableExists("ANTONIOLI_COLOR_SIZES_TO_BUYMA"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table ANTONIOLI_COLOR_SIZES_TO_BUYMA not exist");
				throw new Exception("Source table ANTONIOLI_COLOR_SIZES_TO_BUYMA not exists in DB "+sqlLiteDB.getDbFile());
			}
			
			//************ ANTONIOLI END CHECK TABLES **************
			
			//truncate the destination tables
			String sqlStatement = "DELETE FROM BUYMA_ITEMS_EXPORT;";
			if(logger!=null)
				logger.log(Level.INFO, "Trucate the table BUYMA_ITEMS_EXPORT");
			sqlLiteDB.executeUpdate(sqlStatement);
			
			sqlStatement = "DELETE FROM BUYMA_COLOR_SIZES_EXPORT;";
			if(logger!=null)
				logger.log(Level.INFO, "Trucate the table BUYMA_COLOR_SIZES_EXPORT");
			sqlLiteDB.executeUpdate(sqlStatement);
			
			//add records to destinations tables from ANTONIOLI
			sqlStatement = ""+
			"INSERT INTO BUYMA_ITEMS_EXPORT	(" +
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
					"\"コントロール値\","+
					"\"単価\","+
					"\"買付可数量\","	+
					"\"購入期限\","+
					"\"参考価格/通常出品価格\","+
					"\"参考価格\","+
					"\"商品コメント\","+
					"\"色サイズ補足\","+
					"\"連絡事項(色・サイズ)※削除項目\","+
					"\"タグ\","+
					"\"コントロール値2\","+
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
					" SELECT * FROM ANTONIOLI_ITEMS_TO_BUYMA group by 商品管理番号;";
			if(logger!=null)
				logger.log(Level.FINE, "Insert Antonioli records into the table BUYMA_ITEMS_EXPORT.");
			sqlLiteDB.executeUpdate(sqlStatement);
			
			sqlStatement = " "+
					"INSERT INTO BUYMA_COLOR_SIZES_EXPORT ("+            	
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
					" SELECT * FROM ANTONIOLI_COLOR_SIZES_TO_BUYMA group by 商品管理番号, サイズ名称;";
					if(logger!=null)
						logger.log(Level.FINE, "Insert Antonioli records into the table BUYMA_COLOR_SIZES_EXPORT.");
					sqlLiteDB.executeUpdate(sqlStatement);
			
			if(logger!=null)
				logger.log(Level.FINE, "End loadFinalCSVExportTable method.");
		}
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
		}
		finally
		{
			if(sqlLiteDB != null && sqlLiteDB.isConnetionOpen())
				sqlLiteDB.closeDBConnection();
		}	
		
	}
	
	/***
	 * The method loads csv files zipped in zip file. It looks for the most recent zip file in the folder.
	 * @param zipFilePath folder where to look for the zip file
	 * @param separator csv separator character
	 * @param truncate if true the destination tables will be truncated
	 * @param haveHeader if true the csv files have the header as firts row
	 * @param archiveFiles if true the zip file will be moved in a subfolder named processed
	 * @throws Exception
	 */
	public void loadCurrentBuyMafromZip(String zipFilePath, char separator, boolean truncate, boolean haveHeader, boolean archiveFiles) throws Exception
	{
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start loadCurrentBuyMafromZip method.");
			
			//look for all zips in folder
			File path = new File(zipFilePath);
			FilenameFilter zips = new FilenameFilter() { 
				  
                public boolean accept(File f, String name) 
                { 
                    return name.endsWith(".zip");
                } 
            }; 
           	File[] files = path.listFiles(zips);
           	if(files.length == 0)
           	{
           		if(logger!=null)
    				logger.log(Level.WARNING, "No zip files found!");
           		return;
           	}
            if(logger!=null)
				logger.log(Level.INFO, "Found "+files.length+ " zip files in folder "+zipFilePath);
            Arrays.sort(files, new Comparator<File>() {
                public int compare(File f1, File f2) {
                    return Long.compare(f1.lastModified(), f2.lastModified());
                }
            });
            if(logger!=null)
				logger.log(Level.INFO, "Choosed the most recent one: "+files[0].getName());
            
            if(logger!=null)
				logger.log(Level.INFO, "Unzip the file: "+files[0].getName()+" in the same directory.");
            
			File zipFile = files[0];
			unzip(zipFile.getPath(),zipFile.getParent());
			
			loadCurrentBuyMaFromCSV(zipFilePath+"\\items.utf8.csv", zipFilePath+"\\colorsizes.utf8.csv", separator, truncate, haveHeader, false);
			
			if(logger!=null)
				logger.log(Level.INFO, "Delete unziped cvs files.");
			
			FilenameFilter csvs = new FilenameFilter() { 
				  
                public boolean accept(File f, String name) 
                { 
                    return name.endsWith(".csv");
                } 
            }; 
           	File[] csvFiles = path.listFiles(csvs);
           	Path pfile;
           	for(int i = 0; i < csvFiles.length; i++)
           	{
           		pfile = Paths.get(csvFiles[i].getPath());
           		Files.delete(pfile);
           	}
           	
           	if(archiveFiles)
           	{
           		if(logger!=null)
					logger.log(Level.INFO, "Move zip file to processed subfolder.");
				
				File processedFolder = new File(files[0].getParent()+"\\processed");
				if(!processedFolder.exists())
					processedFolder.mkdir();
				
				// append TimeStamp
				
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
				LocalDateTime date =  LocalDateTime.now();
				Path destination;				
				int extensionIndex = (zipFile.getName()).indexOf(".zip");
				if(extensionIndex!=-1)
					destination = Paths.get(zipFile.getParent()+"\\processed\\"+(zipFile.getName()).substring(0,extensionIndex)+"_"+date.format(formatter)+".zip");
				else
					destination  = Paths.get(zipFile.getParent()+"\\processed\\"+zipFile.getName()+"_"+date.format(formatter)+".zip"); 
				Path source = Paths.get(zipFile.getPath());
				Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
				
				if(logger!=null)
					logger.log(Level.INFO, "zip file moved.");	
           	}
			
			if(logger!=null)
				logger.log(Level.FINE, "End loadCurrentBuyMafromZip method.");
		}
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
		}
	}
	
	/***
	 * Load into BUYMA_ITEMS and BUYMA_COLOR_SIZES data from csv files
	 * @param originPathNameItems path and name of csv file items
	 * @param originPathNameColorSizes path and name of csv file colorsizes
	 * @param separator
	 * @param truncate if true data are deleted in the destinations tables before to load the new one
	 * @param haveHeader if true the first line won't be considered
	 * @param archiveFilse if true moves the file to a subfolder named processed
	 * @throws Exception 
	 */
	public void loadCurrentBuyMaFromCSV(String originPathNameItems, String originPathNameColorSizes, char separator, boolean truncate, boolean haveHeader, boolean archiveFiles) throws Exception
	{
		String statement = "";
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start loadCurrentBuyMaFromCSV method.");
			//connect to db
			if(logger!=null)
				logger.log(Level.INFO, "Open the db: "+sqlLiteDB.getDbFile());
			sqlLiteDB.connectToDB();			
			
			//check if origin files exist
			if(logger!=null)
				logger.log(Level.FINE, "Check if items origin file "+originPathNameItems+" exists");
			File f_items = new File(originPathNameItems); 
			if (!f_items.exists())
			{
				if(logger!=null)
					logger.log(Level.WARNING, "file "+originPathNameItems+" not exists");
				return;
			}
				
			if(logger!=null)
				logger.log(Level.FINE, "Check if colorsizes origin file "+originPathNameColorSizes+" exists");
			File f_color_sizes = new File(originPathNameColorSizes); 
			if (!f_color_sizes.exists())
			{
				if(logger!=null)
					logger.log(Level.WARNING, "file "+originPathNameColorSizes+" not exists");
				return;
			}	
						
			//check if destination tables exist
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_ITEMS exists.");
			if(!sqlLiteDB.tableExists("BUYMA_ITEMS"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_ITEMS not exist.");
				throw new Exception("Source table BUYMA_ITEMS not exists in DB "+sqlLiteDB.getDbFile());
			}
			
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_COLOR_SIZES exists.");
			if(!sqlLiteDB.tableExists("BUYMA_COLOR_SIZES"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_COLOR_SIZES not exist.");
				throw new Exception("Source table BUYMA_COLOR_SIZES not exists in DB "+sqlLiteDB.getDbFile());
			}
			
			//start with items
			
			if(truncate)
			{
				if(logger!=null)
					logger.log(Level.INFO, "Truncate BUYMA_ITEMS table.");
				sqlLiteDB.executeUpdate("DELETE FROM BUYMA_ITEMS;");
			}
			
			if(logger!=null)
				logger.log(Level.INFO, "Set up items CSV parser.");
			
			Reader reader =  new FileReader(originPathNameItems);
	            
			CSVParser parser = new CSVParserBuilder()
				    .withSeparator(separator)
				    .withIgnoreQuotations(false)
				    .build();
			
			CSVReader csvReader = null;			
			
			if(haveHeader)
            {
            	if(logger!=null)
					logger.log(Level.FINE, "Skip items CSV first line.");
            	csvReader = new CSVReaderBuilder(reader)
    				    .withSkipLines(1)
    				    .withCSVParser(parser)
    				    .build();
            }
			else
				csvReader = new CSVReaderBuilder(reader)
			    .withCSVParser(parser)
			    .build();
			
			String[] line;
			if(logger!=null)
				logger.log(Level.INFO, "Read items CSV: "+originPathNameItems);
			
            while ((line = csvReader.readNext()) != null) 
            {
            	if(logger!=null)
    				logger.log(Level.FINE, "Escape quote char in fields.");
            	for(int i=0; i<line.length; i++)
            		line[i] = line[i].replaceAll("'", "''");
            	
            	statement = "INSERT INTO BUYMA_ITEMS (" +
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
            			"'"+line[0]+"',"+
            			"'"+line[1]+"',"+
            			"'"+line[2]+"',"+
            			"'"+line[3]+"',"+
            			"'"+line[4]+"',"+
            			"'"+line[5]+"',"+
            			"'"+line[6]+"',"+
            			"'"+line[7]+"',"+
            			"'"+line[8]+"',"+
            			"'"+line[9]+"',"+
            			"'"+line[10]+"',"+
            			"'"+line[11]+"',"+
            			"'"+line[12]+"',"+
            			"'"+line[13]+"',"+
            			"'"+line[14]+"',"+
            			"'"+line[15]+"',"+
            			"'"+line[16]+"',"+
            			"'"+line[17]+"',"+
            			"'"+line[18]+"',"+
            			"'"+line[19]+"',"+
            			"'"+line[20]+"',"+
            			"'"+line[21]+"',"+
            			"'"+line[22]+"',"+
            			"'"+line[23]+"',"+
            			"'"+line[24]+"',"+
            			"'"+line[25]+"',"+
            			"'"+line[26]+"',"+
            			"'"+line[27]+"',"+
            			"'"+line[28]+"',"+
            			"'"+line[29]+"',"+
            			"'"+line[30]+"',"+
            			"'"+line[31]+"',"+
            			"'"+line[32]+"',"+
            			"'"+line[33]+"',"+
            			"'"+line[34]+"',"+
            			"'"+line[35]+"',"+
            			"'"+line[36]+"',"+
            			"'"+line[37]+"',"+
            			"'"+line[38]+"',"+
            			"'"+line[39]+"',"+
            			"'"+line[40]+"',"+
            			"'"+line[41]+"',"+
            			"'"+line[42]+"',"+
            			"'"+line[43]+"',"+
            			"'"+line[44]+"',"+
            			"'"+line[45]+"',"+
            			"'"+line[46]+"',"+
            			"'"+line[47]+"',"+
            			"'"+line[48]+"',"+
            			"'"+line[49]+"',"+
            			"'"+line[50]+"',"+
            			"'"+line[51]+"',"+
            			"'"+line[52]+"',"+
            			"'"+line[53]+"',"+
            			"'"+line[54]+"',"+
            			"'"+line[55]+"',"+
            			"'"+line[56]+"',"+
            			"'"+line[57]+"',"+
            			"'"+line[58]+"',"+
            			"'"+line[59]+"',"+
            			"'"+line[60]+"',"+
            			"'"+line[61]+"',"+
            			"'"+line[62]+"',"+
            			"'"+line[63]+"',"+
            			"'"+line[64]+"',"+
            			"'"+line[65]+"',"+
            			"'"+line[66]+"',"+
            			"'"+line[67]+"',"+
            			"'"+line[68]+"',"+
            			"'"+line[69]+"',"+
            			"'"+line[70]+"',"+
            			"'"+line[71]+"',"+
            			"'"+line[72]+"',"+
            			"'"+line[73]+"',"+
            			"'"+line[74]+"',"+
            			"'"+line[75]+"',"+
            			"'"+line[76]+"',"+
            			"'"+line[77]+"',"+
            			"'"+line[78]+"',"+
            			"'"+line[79]+"',"+
            			"'"+line[80]+"',"+
            			"'"+line[81]+"',"+
            			"'"+line[82]+"',"+
            			"'"+line[83]+"',"+
            			"'"+line[84]+"',"+
            			"'"+line[85]+"',"+
            			"'"+line[86]+"',"+
            			"'"+line[87]+"',"+
            			"'"+line[88]+"',"+
            			"'"+line[89]+"',"+
            			"'"+line[90]+"',"+
            			"'"+line[91]+"',"+
            			"'"+line[92]+"',"+
            			"'"+line[93]+"',"+
            			"'"+line[94]+"',"+
            			"'"+line[95]+"',"+
            			"'"+line[96]+"',"+
            			"'"+line[97]+"',"+
            			"'"+line[98]+"',"+
            			"'"+line[99]+"',"+
            			"'"+line[100]+"',"+
            			"'"+line[101]+"',"+
            			"'"+line[102]+"',"+
            			"'"+line[103]+"',"+
            			"'"+line[104]+"',"+
            			"'"+line[105]+"',"+
            			"'"+line[106]+"',"+
            			"'"+line[107]+"',"+
            			"'"+line[108]+"',"+
            			"'"+line[109]+"',"+
            			"'"+line[110]+"',"+
            			"'"+line[111]+"');";
            	if(logger!=null)
    				logger.log(Level.FINE, "Insert csv line into table BUYMA_ITEMS.");
            	sqlLiteDB.executeUpdate(statement);
            }//while	
            if(logger!=null)
				logger.log(Level.INFO, "Csv items loaded into table BUYMA_ITEMS.");
            
            csvReader.close();
            reader.close();
            
            //color sizes file
            
            if(truncate)
			{
				if(logger!=null)
					logger.log(Level.INFO, "Truncate BUYMA_COLOR_SIZES table.");
				sqlLiteDB.executeUpdate("DELETE FROM BUYMA_COLOR_SIZES;");
			}
            
            if(logger!=null)
				logger.log(Level.INFO, "Set up color_sizes CSV parser.");
			
			reader =  new FileReader(originPathNameColorSizes);
			
			if(haveHeader)
            {
            	if(logger!=null)
					logger.log(Level.FINE, "Skip items CSV first line.");
            	csvReader = new CSVReaderBuilder(reader)
    				    .withSkipLines(1)
    				    .withCSVParser(parser)
    				    .build();
            }
			else
				csvReader = new CSVReaderBuilder(reader)
						.withCSVParser(parser)
						.build();
			
			if(logger!=null)
				logger.log(Level.INFO, "Read items CSV: "+originPathNameColorSizes);
			
			while ((line = csvReader.readNext()) != null) 
            {
            	if(logger!=null)
    				logger.log(Level.FINE, "Escape quote char in fields.");
            	for(int i=0; i<line.length; i++)
            		line[i] = line[i].replaceAll("'", "''");
            	statement = "INSERT INTO BUYMA_COLOR_SIZES ("+
		            	"\"商品ID\","+
		            	"\"商品管理番号\","+
		            	"\"商品名\","+
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
            			"'"+line[0]+"',"+
            			"'"+line[1]+"',"+
            			"'"+line[2]+"',"+
            			"'"+line[3]+"',"+
            			"'"+line[4]+"',"+
            			"'"+line[5]+"',"+
            			"'"+line[6]+"',"+
            			"'"+line[7]+"',"+
            			"'"+line[8]+"',"+
            			"'"+line[9]+"',"+
            			"'"+line[10]+"',"+
            			"'"+line[11]+"',"+
            			"'"+line[12]+"',"+
            			"'"+line[13]+"',"+
            			"'"+line[14]+"',"+
            			"'"+line[15]+"',"+
            			"'"+line[16]+"',"+
            			"'"+line[17]+"',"+
            			"'"+line[18]+"',"+
            			"'"+line[19]+"',"+
            			"'"+line[20]+"',"+
            			"'"+line[21]+"',"+
            			"'"+line[22]+"',"+
            			"'"+line[23]+"',"+
            			"'"+line[24]+"',"+
            			"'"+line[25]+"',"+
            			"'"+line[26]+"',"+
            			"'"+line[27]+"',"+
            			"'"+line[28]+"',"+
            			"'"+line[29]+"',"+
            			"'"+line[30]+"',"+
            			"'"+line[31]+"',"+
            			"'"+line[32]+"',"+
            			"'"+line[33]+"',"+
            			"'"+line[34]+"',"+
            			"'"+line[35]+"',"+
            			"'"+line[36]+"',"+
            			"'"+line[37]+"',"+
            			"'"+line[38]+"',"+
            			"'"+line[39]+"',"+
            			"'"+line[40]+"',"+
            			"'"+line[41]+"',"+
            			"'"+line[42]+"',"+
            			"'"+line[43]+"',"+
            			"'"+line[44]+"',"+
            			"'"+line[45]+"',"+
            			"'"+line[46]+"',"+
            			"'"+line[47]+"',"+
            			"'"+line[48]+"',"+
            			"'"+line[49]+"',"+
            			"'"+line[50]+"',"+
            			"'"+line[51]+"',"+
            			"'"+line[52]+"',"+
            			"'"+line[53]+"');";            	
            	if(logger!=null)
    				logger.log(Level.FINE, "Insert csv line into table BUYMA_COLOR_SIZES.");
            	sqlLiteDB.executeUpdate(statement);
            }//while	
            			
			if(logger!=null)
				logger.log(Level.INFO, "Csv color sizes loaded into table BUYMA_COLOR_SIZES.");
			 
			csvReader.close();
			reader.close();
			
			if(archiveFiles)
			{
				if(logger!=null)
					logger.log(Level.INFO, "Move files to processed subfolder.");
				
				File processedFolder = new File(f_items.getParent()+"\\processed");
				if(!processedFolder.exists())
					processedFolder.mkdir();
				
				// append TimeStamp
				
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
				LocalDateTime date =  LocalDateTime.now();
				Path destination;				
				int extensionIndex = (f_items.getName()).indexOf(".csv");
				if(extensionIndex!=-1)
					destination = Paths.get(f_items.getParent()+"\\processed\\"+(f_items.getName()).substring(0,extensionIndex)+"_"+date.format(formatter)+".csv");
				else
					destination  = Paths.get(f_items.getParent()+"\\processed\\"+f_items.getName()+"_"+date.format(formatter)+".csv"); 
				Path source = Paths.get(f_items.getPath());
				Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
				
				extensionIndex = (f_color_sizes.getName()).indexOf(".csv");
				if(extensionIndex!=-1)
					destination = Paths.get(f_color_sizes.getParent()+"\\processed\\"+(f_color_sizes.getName()).substring(0,extensionIndex)+"_"+date.format(formatter)+".csv");
				else
					destination  = Paths.get(f_items.getParent()+"\\processed\\"+f_color_sizes.getName()+"_"+date.format(formatter)+".csv"); 
				source = Paths.get(f_color_sizes.getPath());
				Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
				
				if(logger!=null)
					logger.log(Level.INFO, "csv files moved.");					
			}
			
			if(logger!=null)
				logger.log(Level.FINE, "End loadCurrentBuyMaFromCSV method.");			
		}
		catch (CsvException csve)
		{
			throw new Exception(csve.getMessage());
		}
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
		}
		finally 
    	{
    		if(sqlLiteDB.isConnetionOpen())
    		{
    			sqlLiteDB.closeDBConnection();
    		}
        }
	}//loadCurrentBuyMaFromCSV
	
	protected void unzip(String zipFilePathName, String destDir) throws Exception {
		ZipInputStream zis = null;
		FileInputStream fis = null;
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start unzip method.");
			
	        File dir = new File(destDir);
	        // create output directory if it doesn't exist
	        if(!dir.exists()) 
	        	dir.mkdirs();
	        
	        //buffer for read and write data to file
	        byte[] buffer = new byte[1024];
        
            fis = new FileInputStream(zipFilePathName);
            zis = new ZipInputStream(fis);
            ZipEntry ze = zis.getNextEntry();
            while(ze != null)
            {
                String fileName = ze.getName();
                File newFile = new File(destDir + File.separator + fileName);
                logger.log(Level.INFO,"Unzipping file to "+newFile.getAbsolutePath());
                //create directories for sub directories in zip
                new File(newFile.getParent()).mkdirs();
                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) 
                {
                	fos.write(buffer, 0, len);
                }
                fos.close();
                //close this ZipEntry
                zis.closeEntry();
                ze = zis.getNextEntry();
            }
            //close last ZipEntry
            zis.closeEntry();
            zis.close();
            fis.close();
            
            if(logger!=null)
				logger.log(Level.FINE, "End unzip method.");
        } 
        catch (Exception e) 
		{
        	throw new Exception(e.getMessage());
        }     
		finally 
    	{
    		if(zis != null)
    			zis.close();
    		if(fis != null)
    			fis.close();
        }
    }
}//class
