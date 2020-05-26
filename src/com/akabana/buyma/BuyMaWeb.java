package com.akabana.buyma;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/*
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
*/

import java.util.logging.Level;
import java.util.logging.Logger;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.Select;

/*
import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.History;
import com.gargoylesoftware.htmlunit.HttpMethod;
import com.gargoylesoftware.htmlunit.SilentCssErrorHandler;
import com.gargoylesoftware.htmlunit.TextPage;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.Html;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlForm;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.util.NameValuePair;

import java.net.URL;
import com.twocaptcha.api.TwoCaptchaService;
*/
public class BuyMaWeb {
	
	private String webSiteLink = "";
	private Logger logger = null;
	private String chromeDriverPathName = "";
	private String chromeUserDataPath = "";
	private String downloadFilesPath = "";
	private WebDriver driver = null;
	
	//String apiKey = "e59748eb3b5c6c8df318c1058a0174bf";
	//String googleKey = "6Le50LcUAAAAALVbLvJ_Oq7brlxJqcowAlwlw1CK";
	
	public BuyMaWeb(String webSiteLink, String chromeDriverPathName)
	{
		this.webSiteLink = webSiteLink;
		this.chromeDriverPathName = chromeDriverPathName;
	}
	
	public BuyMaWeb(String webSiteLink, String chromeDriverPathName, String chromeUserDataPath)
	{
		this(webSiteLink, chromeDriverPathName);
		this.chromeUserDataPath = chromeUserDataPath;
	}
	
	public BuyMaWeb(String webSiteLink, String chromeDriverPathName, Logger logger)
	{
		this(webSiteLink, chromeDriverPathName);
		this.logger = logger;
	}
	
	public BuyMaWeb(String webSiteLink, String chromeDriverPathName, String chromeUserDataPath, Logger logger)
	{
		this(webSiteLink, chromeDriverPathName, chromeUserDataPath);
		this.logger = logger;
	}	
		
	/***
	 * Log in buyma Web
	 * @param loginPage the web page address that will be concatenated to the root website to get the page address
	 * @param username username used to log in
	 * @param password password used to log in
	 * @return if true logged in successfully
	 * @throws Exception
	 */
	public void BuyMaLogin(String loginPage, String username, String password) throws Exception
	{
		//try
		//{
			
			// Init chromedriver
			//String chromeDriverPath = "C:\\Users\\tmorello\\WebScraperWorkspace\\chromedriver_win32\\chromedriver.exe" ;
			//System.setProperty("webdriver.chrome.driver", chromeDriverPath);
			//ChromeOptions options = new ChromeOptions();
			//options.addArguments("user-data-dir=C:\\Users\\tmorello\\AppData\\Local\\Google\\Chrome\\User Data\\");
			//options.addArguments("--start-maximized");
			//WebDriver driver = new ChromeDriver(options);
			//driver.manage().timeouts().implicitlyWait(5,TimeUnit.SECONDS);
			if(logger!=null)
				logger.log(Level.FINE, "Start BuyMaLogin method.");
			if(driver == null)
			{
				if(logger!=null)
					logger.log(Level.FINE, "Web browser not started yet.");
				startWebDriver();
			}
			
			if(logger!=null)
				logger.log(Level.INFO, "Get the login page.");		
			driver.get(webSiteLink+loginPage);
			
			if(logger!=null)
				logger.log(Level.INFO, "Compile username and password and click login button.");
			WebElement form = driver.findElement(By.name("formlogin"));
			form.findElement(By.name("txtLoginId")).sendKeys(username);
			Thread.sleep(3000);
			form.findElement(By.name("txtLoginPass")).sendKeys(password);
			Thread.sleep(1200);
			form.findElement(By.xpath("//input[@value='ログイン']")).click();
			Thread.sleep(5000);
			/*
			if(driver.getCurrentUrl().contains(webSiteLink+loginPage))
				return false;
			else
				return true;
			*/
		/*
		}
		catch (Exception e) 
		{
			if(driver != null)
				driver.close();
            throw new Exception(e.getMessage());
        }	
        */
	}//BuyMaLogin 
	
	/***
	 * Uploads the zip files with items to the upload page. Log in is required before this.
	 * @param downloadWebPage the web page address that will be concatenated to the root website to get the page address 
	 * @param zipFilePathName path and name of zip file
	 * @throws Exception
	 */
	public void uploadZipToBuyMa(String downloadWebPage, String zipFilePathName) throws Exception
	{
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start uploadZipToBuyMa method.");
			if(driver == null)
				throw new Exception("Browser not started yet!");
			
			if(logger!=null)
				logger.log(Level.INFO, "Navigate to the download page.");
			driver.navigate().to(webSiteLink+downloadWebPage);
			Thread.sleep(5000);
			
			if(logger!=null)
				logger.log(Level.INFO, "Click upload button.");
			driver.findElement(By.linkText("商品リストをアップロードする")).click();
			Thread.sleep(1200);
			
			if(logger!=null)
				logger.log(Level.INFO, "Select zip file type from drop down menu.");
			new Select(driver.findElement(By.id("filetype"))).selectByVisibleText("zipファイルでまとめてアップロード");
			Thread.sleep(1200);
			
			if(logger!=null)
				logger.log(Level.INFO, "Click file dialog open button.");
			WebElement inputfile = driver.findElement(By.id("zip"));
			inputfile.sendKeys(zipFilePathName);
			
			Thread.sleep(1200);
			
			driver.findElement(By.name("upload")).click();
			
			Thread.sleep(3000);
			
			driver.findElement(By.linkText("履歴画面に遷移")).click();
			
			Thread.sleep(2000);
			
			if(logger!=null)
				logger.log(Level.FINE, "End uploadZipToBuyMa method.");
		}
		catch (Exception e) 
		{
			if(driver != null)
				driver.quit();
            throw new Exception(e.getMessage());
        }	
	}
	
	/***
	 * download the items present in buyma as csv zipped files. Log in is required before this.
	 * @param downloadWebPage the web page address that will be concatenated to the root website to get the page address 
	 * @throws Exception 
	 */
	public void downloadAllBuyMaCatalog(String activeitemsPage, String downloadWebPage) throws Exception
	{
		try
		{
			if(logger!=null)
				logger.log(Level.FINE, "Start downloadAllBuyMaCatalog method.");
			if(driver == null)
				throw new Exception("Browser not started yet!");
			
			if(logger!=null)
				logger.log(Level.INFO, "Navigate to the active items page.");
			driver.navigate().to(webSiteLink+activeitemsPage);
			Thread.sleep(5000);
			
			if(logger!=null)
				logger.log(Level.FINE, "Click download button.");
			driver.findElement(By.id("js-sell-bulk-export-button")).click();
			Thread.sleep(5000);
			
			if(logger!=null)
				logger.log(Level.FINE, "Choose to download all.");
			driver.findElement(By.cssSelector("label[for='sell-bulk-export-dialog__preset_1']")).click();
			Thread.sleep(1000);
			
            if(logger!=null)
				logger.log(Level.FINE, "Confirm download.");
            driver.findElement(By.xpath("//input[@value='ダウンロードを準備']")).click();
			
            if(logger!=null)
				logger.log(Level.FINE, "Wait some seconds the dwonload to be ready.");
			Thread.sleep(60000);
			
			if(logger!=null)
				logger.log(Level.INFO, "Navigate to the download page.");
			driver.navigate().to(webSiteLink+downloadWebPage);			
					
			if(logger!=null)
				logger.log(Level.INFO, "Click on first download button found.");
			driver.findElement(By.xpath("//a[starts-with(@href, 'https://www.buyma.com/my/sell/bulk/') and contains(@href, '/download/exported')]")).click();
		}
		catch (Exception e) 
		{
			if(driver != null)
				driver.quit();
            throw new Exception(e.getMessage());
        }	
	}//downloadAllBuyMaCatalog
	
	protected void startWebDriver() throws Exception
	{
		try
    	{
			//light web browser start
			if(logger!=null)
				logger.log(Level.FINE, "Start the Chrome Web Browser");
			if(this.chromeDriverPathName.equals(""))
				throw new Exception("Chrome Driver path not set!");
			System.setProperty("webdriver.chrome.driver", this.chromeDriverPathName);
			ChromeOptions options = new ChromeOptions();
			if(!this.chromeUserDataPath.equals(""))
				options.addArguments("user-data-dir="+this.chromeUserDataPath);
			options.addArguments("--start-maximized");
			
			//add default download directory
			Map<String, Object> prefs = new HashMap<String, Object>();
			prefs.put("profile.default_content_settings.popups", 0);
			if(!this.downloadFilesPath.equals(""))
				prefs.put("download.default_directory", this.downloadFilesPath);
			prefs.put("extensions_to_open", "");
			prefs.put("directory_upgrade", "True");
			options.setExperimentalOption("prefs", prefs);
								
			driver = new ChromeDriver(options);
			driver.manage().timeouts().implicitlyWait(5,TimeUnit.SECONDS);
			if(logger!=null)
				logger.log(Level.FINE, "Chrome Web Browser started.");
    	}
		catch (Exception e) 
		{
			if(driver != null)
				driver.quit();
            throw new Exception(e.getMessage());
        }		
	}//startWebDriver
	
	public void closeBrowser() throws Exception
	{
		try
    	{
			if(logger!=null)
				logger.log(Level.FINE, "Close the Chrome Web Browser");
			if(this.driver !=null)
				this.driver.quit();
    	}
		catch (Exception e) 
		{
			throw new Exception(e.getMessage());
        }	
	}
	
	public String getChromeDriverPathName()
	{
		return this.chromeDriverPathName;
	}
	
	public String getChromeUserDataPath()
	{
		return this.chromeUserDataPath;
	}
	
	public void setChromeUserDataPath(String chromeUserDataPath)
	{
		this.chromeUserDataPath = chromeUserDataPath; 
	}
	
	public void setDownloadFilesPath(String downloadFilesPath)
	{
		this.downloadFilesPath = downloadFilesPath;
	}
	
	public String getDownloadFilesPath()
	{
		return this.downloadFilesPath;
	}
}//class
