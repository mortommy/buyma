package com.akabana.buyma;

import java.util.Arrays;
import java.util.Comparator;
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
	 * Starting from the source table (ANTONIOLI) loads data to BuyMa tables (ANTONIOLI_ITEMS_FOR_BUYMA; 
	 * ANTONIOLI_COLOR_SIZES_FOR_BUYMA) using current items loaded in BuyMa (thanks to tables BUYMA_ITEMS
	 * and BUYMA_COLOR_SIZES)
	 * @throws Exception 
	 */
	/*public void loadBuyMaTableFromAntonioli() throws Exception
	{
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start loadBuyMaTableFromAntonioli method.");
			
			//connect to db
			if(logger!=null)
				logger.log(Level.INFO, "Open the db: "+sqlLiteDB.getDbFile());
			sqlLiteDB.connectToDB();
			
			//check if source table exists
			if(logger!=null)
				logger.log(Level.FINE, "Check if table ANTONIOLI exists");
			if(!sqlLiteDB.tableExists("ANTONIOLI"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table ANTONIOLI not exist");
				throw new Exception("Source table ANTONIOLI not exists in DB "+sqlLiteDB.getDbFile());
			}
			
			//check if destination tables exists
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
			
			//check if support tables exists
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_ITEMS exists");
			if(!sqlLiteDB.tableExists("BUYMA_ITEMS"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_ITEMS not exist");
				throw new Exception("Source table BUYMA_ITEMS not exists in DB "+sqlLiteDB.getDbFile());
			}
			if(logger!=null)
				logger.log(Level.FINE, "Check if table BUYMA_COLOR_SIZES exists");
			if(!sqlLiteDB.tableExists("BUYMA_COLOR_SIZES"))
			{
				if(logger!=null)
					logger.log(Level.SEVERE, "Table BUYMA_COLOR_SIZES not exist");
				throw new Exception("Source table BUYMA_COLOR_SIZES not exists in DB "+sqlLiteDB.getDbFile());
			}
			
			//truncate the destination tables
			String sqlStatement = "DELETE FROM ANTONIOLI_COLOR_SIZES_TO_BUYMA;";
			if(logger!=null)
				logger.log(Level.INFO, "Trucate the table ANTONIOLI_COLOR_SIZES_TO_BUYMA");
			sqlLiteDB.executeUpdate(sqlStatement);
			
			sqlStatement = "DELETE FROM ANTONIOLI_ITEMS_TO_BUYMA;";
			if(logger!=null)
				logger.log(Level.INFO, "Trucate the table ANTONIOLI_ITEMS_TO_BUYMA");
			sqlLiteDB.executeUpdate(sqlStatement);
			
			//MANAGE ITEM TO BE DELETED
			sqlStatement = ""+
					"INSERT INTO ANTONIOLI_COLOR_SIZES_TO_BUYMA ("+            	
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
					" SELECT "+
	            	"buy.\"商品ID\","+
	            	"buy.\"商品管理番号\","+
	            	"buy.\"商品名\","+
	            	"'停止',"+
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
	            	"WHERE ant.STATUS = "+StagingRecordStatus.DELETED+" "+
	                "AND ant.BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+";";
			if(logger!=null)
				logger.log(Level.INFO, "Insert into the table ANTONIOLI_COLOR_SIZES_TO_BUYMA items with 0 stock.");
			sqlLiteDB.executeUpdate(sqlStatement);
			
			//check if all sizes of an item have been deleted
			//in that case we have to deactivate the item
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
					"\"商品イメージ 1\","+
					"\"商品イメージ 2\","+
					"\"商品イメージ 3\","+
					"\"商品イメージ 4\","+
					"\"商品イメージ 5\","+
					"\"商品イメージ 6\","+
					"\"商品イメージ 7\","+
					"\"商品イメージ 8\","+
					"\"商品イメージ 9\","+
					"\"商品イメージ 10\","+
					"\"商品イメージ 11\","+
					"\"商品イメージ 12\","+
					"\"商品イメージ 13\","+
					"\"商品イメージ 14\","+
					"\"商品イメージ 15\","+
					"\"商品イメージ 16\","+
					"\"商品イメージ 17\","+
					"\"商品イメージ 18\","+
					"\"商品イメージ 19\","+
					"\"商品イメージ 20\","+
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
			sqlLiteDB.executeUpdate(sqlStatement);
			
			//update the buyma status
			sqlStatement = "UPDATE ANTONIOLI "+
					"SET BUYMA_STATUS= "+StagingBuymaStatus.PROCESSED+" "+
					"WHERE (STATUS="+StagingRecordStatus.DELETED+" "+ 
					"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
			if(logger!=null)
				logger.log(Level.INFO, "Update records into table ANTONIOLI with 0 stock as processed.");
			sqlLiteDB.executeUpdate(sqlStatement);
			
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
					"an.ITEM_LINK || ' - ' || CAST(CAST(substr(CAST((ROUND((CAST(an.ITEM_PRICE as REAL))/1.22,2)-(ROUND((CAST(an.ITEM_PRICE as REAL))/1.22,2)*0.2)) as TEXT),1,instr(CAST((ROUND((CAST(an.ITEM_PRICE as REAL))/1.22,2)-(ROUND((CAST(an.ITEM_PRICE as REAL))/1.22,2)*0.2)) as TEXT),'.')-1) as REAL) +1 as TEXT), "+
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
			sqlLiteDB.executeUpdate(sqlStatement);
			
			//update the buyma status
			sqlStatement = "UPDATE ANTONIOLI "+
					"SET BUYMA_STATUS= "+StagingBuymaStatus.PROCESSED+" "+
					"WHERE (STATUS="+StagingRecordStatus.MODIFIED+" "+ 
					"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
			
			if(logger!=null)
				logger.log(Level.INFO, "Update records modified into table ANTONIOLI as processed.");
			sqlLiteDB.executeUpdate(sqlStatement);
			
			//MANAGE NEW ITEMS
			sqlStatement = "SELECT * FROM ANTONIOLI "+
							"WHERE STATUS="+StagingRecordStatus.NEW+" "+
							"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+";";
			if(logger!=null)
				logger.log(Level.FINE, "Get records from the table ANTONIOLI marked as new.");
			ResultSet rs = sqlLiteDB.executeSelect(sqlStatement);
			String currentItemId = "";
			List<String> candidateBrands = new ArrayList<String>();
			String candidateCategory = "";
			String candidateSize = "";
			DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE;
			LocalDateTime expireDate =  LocalDateTime.now();
			expireDate = expireDate.plusDays(14);
			while(expireDate.getDayOfWeek() != DayOfWeek.SUNDAY)
				expireDate = expireDate.plusDays(1);
			int sizeRowCount = 1;
			
			if(logger!=null)
				logger.log(Level.FINE, "Iterate on all items marked as new.");
			while (rs.next()) 
			{				
				if(!currentItemId.equals(rs.getString("ITEM_SKU")))
				{
					currentItemId = rs.getString("ITEM_SKU");
					candidateBrands.clear();
					sizeRowCount = 1;
					
					//new item found, check if it is processable
					if(logger!=null)
						logger.log(Level.FINE, "Start to process the item "+rs.getString("ITEM_SKU"));				
					
					//BRAND
					if(logger!=null)
						logger.log(Level.FINE, "Retreive brands and look for the most suitable.");
					sqlStatement = "SELECT * FROM BUYMA_BRAND_ID;";
					ResultSet brands = sqlLiteDB.executeSelect(sqlStatement);
					int score = 0;
										
					while(brands.next())
					{
						score = FuzzySearch.ratio((rs.getString("ITEM_BRAND")).toUpperCase(),(brands.getString("ブランド名(英語)")).toUpperCase());
						if(score > 98)
						{
							candidateBrands.add(brands.getString("ブランドＩＤ"));
							candidateBrands.add(brands.getString("ブランド名(英語)"));
						}
					}
					if(candidateBrands.size() != 2)
					{
						if(logger!=null)
							logger.log(Level.WARNING, "More than one brand found as suitable, the item will be marked as unprocessable.");
						sqlStatement = "UPDATE ANTONIOLI "+
								"SET BUYMA_STATUS= "+StagingBuymaStatus.UNPROCESSABLE+" "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+
								"AND ITEM_SKU="+rs.getString("ITEM_SKU")+" "+
								"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
						if(logger!=null)
							logger.log(Level.INFO, "Update the record into the tabel ANTONIOLI as unprocessable.");
						sqlLiteDB.executeUpdate(sqlStatement);
						break;
					}
					//CATEGORY
					if(logger!=null)
						logger.log(Level.FINE, "Retreive categories and look for the most suitable.");
					String g = (rs.getString("ITEM_GENDER")=="W") ? "WOMEN" : "MEN";
					sqlStatement = "SELECT * FROM BUYMA_CATEGORY_ID_FROM_ANTONIOLI "+
								"WHERE ANTONIOLI_GENDER='"+g+"' AND "+
								"ANTONIOLI_CATEGORY_2A='"+rs.getString("ITEM_CATEGORY").toUpperCase()+"' OR "+
								"ANTONIOLI_CATEGORY_2B='"+rs.getString("ITEM_CATEGORY").toUpperCase()+"';";
					ResultSet categories = sqlLiteDB.executeSelect(sqlStatement);
					int rowCount = 0;
					//a first iteration used also to count rows
					while(categories.next())
					{
						if(categories.getString("ANTONIOLI_DESC_2A_KEYWORDS").isEmpty() && categories.getString("ANTONIOLI_DESC_2A_KEYWORDS").isEmpty())
							candidateCategory = categories.getString("カテゴリＩＤ");
						rowCount++;
					}
					if(rowCount == 0)
					{
						if(logger!=null)
							logger.log(Level.WARNING, "No category found, the item will be marked as unprocessable.");
						sqlStatement = "UPDATE ANTONIOLI "+
								"SET BUYMA_STATUS= "+StagingBuymaStatus.UNPROCESSABLE+" "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+
								"AND ITEM_SKU="+rs.getString("ITEM_SKU")+" "+
								"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
						if(logger!=null)
							logger.log(Level.INFO, "Update the record into the tabel ANTONIOLI as unprocessable.");
						sqlLiteDB.executeUpdate(sqlStatement);
						break;
					}
					if(rowCount > 1)
					{
						if(logger!=null)
							logger.log(Level.FINE, "Found more than one category, look for keywords.");
						categories.beforeFirst();
						while(categories.next())
						{
							//special case harcoded
							if(rs.getString("ITEM_CATEGORY").contains("WALLETS & CARDHOLDERS"))
							{
								if(logger!=null)
									logger.log(Level.FINE, "Special case category WALLETS & CARDHOLDERS.");
								int postionOfWidth = rs.getString("ITEM_CATEGORY").indexOf("WIDTH");
								int postionOfCM = rs.getString("ITEM_CATEGORY").indexOf("CM", postionOfWidth);
								if(postionOfWidth!=-1 && Integer.parseInt(rs.getString("ITEM_CATEGORY").substring(postionOfWidth+6, postionOfCM-1))>=19)
								{
									candidateCategory = "3408";
								}									
							}
							if(rs.getString("ITEM_DESCRITPION").contains(categories.getString("ANTONIOLI_DESC_2A_KEYWORDS")) || 
							rs.getString("ITEM_DESCRITPION").contains(categories.getString("ANTONIOLI_DESC_2A_KEYWORDS")))
								candidateCategory = categories.getString("カテゴリＩＤ");
						}						
					}
					if(candidateCategory == "")
					{
						if(logger!=null)
							logger.log(Level.WARNING, "No category found, the item will be marked as unprocessable.");
						sqlStatement = "UPDATE ANTONIOLI "+
								"SET BUYMA_STATUS= "+StagingBuymaStatus.UNPROCESSABLE+" "+
								"WHERE (STATUS="+StagingRecordStatus.NEW+" "+
								"AND ITEM_SKU="+rs.getString("ITEM_SKU")+" "+
								"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
						if(logger!=null)
							logger.log(Level.INFO, "Update the record into the tabel ANTONIOLI as unprocessable.");
						sqlLiteDB.executeUpdate(sqlStatement);
						break;
					}
					
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
							"'"+candidateBrands.get(0)+"',"+
							"'"+candidateBrands.get(1)+"',"+
							"'',"+ //model
							"'"+candidateCategory+"',"+	   //category
							"'',"+ //season
							"'',"+ //tema
							"'',"+ //price
							"'1'," + //quantity
							"'"+expireDate.format(formatter)+"',"+ //expire date
							"'0',"+
							"''," +
							"'"+rs.getString("ITEM_DESCRIPTION")+"',"+
							"''," +
							"''," + //tag
							"'213087'," +
							"'2003004'," + 
							"'000'," +
							"''," +
							"'2003004'," +
							"'000'," + 
							"'0'," +
							"'"+rs.getString("ITEM_LINK")+" - ' || CAST(CAST(substr(CAST((ROUND((CAST("+rs.getString("ITEM_PRICE")+" as REAL))/1.22,2)-(ROUND((CAST("+rs.getString("ITEM_PRICE")+" as REAL))/1.22,2)*0.2)) as TEXT),1,instr(CAST((ROUND((CAST("+rs.getString("ITEM_PRICE")+" as REAL))/1.22,2)-(ROUND((CAST("+rs.getString("ITEM_PRICE")+" as REAL))/1.22,2)*0.2)) as TEXT),'.')-1) as REAL) +1 as TEXT),"+
							"'"+rs.getString("ITEM_PICTURE")+"'," +
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
					sqlLiteDB.executeUpdate(sqlStatement);
				}//new item - header			
				
				//look for size code
				if(rs.getString("ITEM_HIERARCHY2").equals("Size (S/M/L)"))
					candidateSize = "0";
				else
				{
					sqlStatement = ""+
							"SELECT DISTINCT BUYMA_SIZE "+
							"FROM BUYMA_SIZE_ID_FROM_ANTONIOLI "+
							"WHERE upper(ANTONIOLI_SIZE_CHART)=upper('"+rs.getString("ITEM_HIERARCHY2")+"') AND"+
							"ANTONIOLI_SIZE='"+rs.getString("ITEM_SIZE")+"';";
					if(((rs.getString("ITEM_CATEGORY")).toUpperCase()).matches("BOOTS|FLATS|LACE_UPS|LOAFERS|PUMPS|SANDALS|SNEAKERS"))
						sqlStatement = sqlStatement + 
							"AND ANTONIOLI_CATEGORY='SHOES'";
					else
						sqlStatement = sqlStatement + 
						"AND ANTONIOLI_CATEGORY='NO SHOES'";
					ResultSet size = sqlLiteDB.executeSelect(sqlStatement);
					while(size.next())
					{
						candidateSize = size.getString("BUYMA_SIZE");
					}
				}//else			
				
				//create record in ANTONIOLI_COLOR_SIZES_TO_BUYMA
				sqlStatement = " "+
						"INSERT INTO ANTONIOLI_COLOR_SIZES_TO_BUYMA ("+            	
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
						"'',"+
						"'"+rs.getString("ITEM_ID")+"',"+
						"'送料込!!★" + rs.getString("ITEM_BRAND") + "★" + rs.getString("ITEM_CATEGORY") +"',"+
						"'下書き',"+
						"'"+Integer.toString(sizeRowCount)+"',"+ //size order code
						"'"+rs.getString("ITEM_SIZE")+"',"+  //size desc from antonioli web site
						"'"+candidateSize+"',"+  //size search key word        
						"'',"+  //color code (temporary null)
						"'',"+  //color code (temporary null)
						"'2',"+
						"'1',"+
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
				sqlLiteDB.executeUpdate(sqlStatement);
				
				if(logger!=null)
					logger.log(Level.FINE, "End to process the item "+rs.getString("ITEM_SKU"));
				
				sizeRowCount++;
			}//while
			
			//update items status as processed
			sqlStatement = "UPDATE ANTONIOLI "+
					"SET BUYMA_STATUS= "+StagingBuymaStatus.PROCESSED+" "+
					"WHERE (STATUS="+StagingRecordStatus.NEW+" "+ 
					"AND BUYMA_STATUS="+StagingBuymaStatus.TO_BE_PROCESSED+");";
			if(logger!=null)
				logger.log(Level.INFO, "Update new records into table ANTONIOLI as processed.");
			sqlLiteDB.executeUpdate(sqlStatement);
			
			sqlLiteDB.closeDBConnection();
			
			if(logger!=null)
				logger.log(Level.FINE, "End loadBuyMaTableFromAntonioli method.");
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
	}//loadBuyMaTableFromAntonioli	
	*/
	/***
	 * Saves a zip files with two CSV files with data coming from tables BUYMA_ITEMS and BUYMA_COLOR_SIZES
	 * @param destinationPathName  path and name of zip file
	 * @param appendTimeStamp if true appends timestamp to the file name
	 * @throws Exception
	 */
	public void generateBuyMaZipCsv(String destinationPathName, boolean appendTimeStamp) throws Exception
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
			generateBuyMaCsv(path+"items.csv", path+"colorsizes.csv",false);
			
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
	public void generateBuyMaCsv(String destinationPathNameItems, String destinationPathNameColorSizes, boolean appendTimeStamp) throws Exception
	{
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start loadCurrentBuyMaFromCSV method.");
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
			ResultSet rs = sqlLiteDB.executeSelect(sqlStatement);
			
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
			CSVWriter writer = new CSVWriter(new FileWriter(destinationPathNameItems));
						
			writer.writeAll(rs, true, true, true);
			writer.close();
			
			sqlStatement = "SELECT * FROM BUYMA_COLOR_SIZES_EXPORT;";
			if(logger!=null)
				logger.log(Level.FINE, "Get records from BUYMA_COLOR_SIZES_EXPORT table.");
			rs = sqlLiteDB.executeSelect(sqlStatement);
			
			if(logger!=null)
				logger.log(Level.INFO, "Write items record to csv file: "+destinationPathNameColorSizes);
			writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(destinationPathNameColorSizes), "utf-8"));
			writer.writeAll(rs, true, true, true);
			writer.close();
			
			sqlLiteDB.closeDBConnection();
			
			if(logger!=null)
				logger.log(Level.FINE, "End loadCurrentBuyMaFromCSV method.");
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
	}
	
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
					" SELECT * FROM ANTONIOLI_ITEMS_TO_BUYMA;";
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
					" SELECT * FROM ANTONIOLI_COLOR_SIZES_TO_BUYMA;";
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
            			"'"+line[52]+"');";            	
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
