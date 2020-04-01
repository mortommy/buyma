package com.akabana.buyma;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.History;
import com.gargoylesoftware.htmlunit.HttpMethod;
import com.gargoylesoftware.htmlunit.SilentCssErrorHandler;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.Html;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlForm;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.util.NameValuePair;

import java.net.URL;
import com.twocaptcha.api.ProxyType;
import com.twocaptcha.api.TwoCaptchaService;

public class BuyMaWeb {
	
	private String webSiteLink;
	private Logger logger;
	
	String apiKey = "e59748eb3b5c6c8df318c1058a0174bf";
	String googleKey = "6Le50LcUAAAAALVbLvJ_Oq7brlxJqcowAlwlw1CK";
	
	public BuyMaWeb(String webSiteLink)
	{
		this.webSiteLink = webSiteLink;
	}
	
	public BuyMaWeb(String webSiteLink, Logger logger)
	{
		this(webSiteLink);
		this.logger = logger;
	}
	
	/***
	 * Login to the website
	 * @param loginPage page relative url, will be added to the webSiteLink
	 * @param username
	 * @param password
	 * @return
	 * @throws Exception 
	 */
	public HtmlPage BuyMaLogin2(String loginPage, String username, String password) throws Exception
	{
		WebClient webClient = null;
		HtmlPage page = null;
		WebRequest requestSettings = null;
		java.net.URL url = new URL("https://www.buyma.com/login/auth/");
		
		if(logger!=null)
			logger.log(Level.FINE, "Start buyMaLogin2 method");
		try
		{
			if(logger!=null)
				logger.log(Level.INFO, "Open the web page: "+webSiteLink+loginPage);
			webClient = StartWebBrowser();
			//webClient.getOptions().setJavaScriptEnabled(false);
			page = (HtmlPage) webClient.getPage(webSiteLink+loginPage);
			webClient.waitForBackgroundJavaScript(20000);
		
			requestSettings = new WebRequest(url, HttpMethod.POST);
			
			List<NameValuePair> response = page.getWebResponse().getResponseHeaders();
			/*
			for (NameValuePair header : response) {
				requestSettings.setAdditionalHeader(header.getName(), header.getValue());
			}
			*/
			requestSettings.setAdditionalHeader("Content-Type", "text/html; charset=utf-8");
			requestSettings.setAdditionalHeader("Cache-Control", "no-cache");
			requestSettings.setAdditionalHeader("Pragma", "no-cache");
			requestSettings.setAdditionalHeader("Origin", "webSiteLink+loginPage");
			requestSettings.setAdditionalHeader("Content-Encoding", "gzip");
			
			HtmlForm form = page.getFormByName("formlogin");
			
			ArrayList<NameValuePair> inputFormParams = new ArrayList<NameValuePair>();
			inputFormParams.add(new NameValuePair("txtLoginId", username));
			inputFormParams.add(new NameValuePair("txtLoginPass", password));
			TwoCaptchaService service = new TwoCaptchaService(apiKey, googleKey, webSiteLink+loginPage);
			String recaptchaResult = service.solveCaptcha();
			inputFormParams.add(new NameValuePair("recaptchaToken", recaptchaResult));			
			inputFormParams.add(new NameValuePair("onetimeticket", form.getInputByName("onetimeticket").getValueAttribute()));
			
			requestSettings.setRequestParameters(inputFormParams);
			
			HtmlPage page2 = webClient.getPage(requestSettings);
		    
		    webClient.waitForBackgroundJavaScript(120000);
		    
		    BufferedWriter writer = new BufferedWriter(new FileWriter("c:\\after_login_buymapage.html"));
		    writer.write(page2.asXml());		     
		    writer.close();
		    
		    if(logger!=null)
				logger.log(Level.FINE, "End buyMaLogin method");
		    return page2;
		}
		catch (Exception e) 
		{
            throw new Exception(e.getMessage());
        }
		finally 
    	{
    		if(webClient != null)
    		{
	            webClient.getCurrentWindow().getJobManager().removeAllJobs();
	            webClient.close();
	            System.gc();
    		}
        }
	}//BuyMaLogin2
	
	/***
	 * Login to the website
	 * @param loginPage page relative url, will be added to the webSiteLink
	 * @param username
	 * @param password
	 * @return
	 * @throws Exception 
	 */
	public HtmlPage BuyMaLogin(String loginPage, String username, String password) throws Exception
	{
		WebClient webClient = null;
		HtmlPage page = null;
		
		if(logger!=null)
			logger.log(Level.FINE, "Start buyMaLogin method");
		try
		{
			if(logger!=null)
				logger.log(Level.INFO, "Open the web page: "+webSiteLink+loginPage);
			webClient = StartWebBrowser();
			//webClient.getOptions().setJavaScriptEnabled(false);
			page = (HtmlPage) webClient.getPage(webSiteLink+loginPage);
			webClient.waitForBackgroundJavaScript(20000);
			HtmlForm form = page.getFormByName("formlogin");
			
			BufferedWriter writer = new BufferedWriter(new FileWriter("c:\\before_login_buymapage.html"));
			writer.write(page.asXml());		     
		    writer.close();
		    
			// Enter login and passwd
			TimeUnit.SECONDS.sleep(5);
		    form.getInputByName("txtLoginId").type(username);
		    TimeUnit.SECONDS.sleep(1);
		    form.getInputByName("txtLoginPass").type(password);
		    		    
		    TwoCaptchaService service = new TwoCaptchaService(apiKey, googleKey, webSiteLink+loginPage);
		    form.getInputByName("recaptchaToken").setValueAttribute(service.solveCaptcha());
		    
		    // Click "Sign In" button
		    if(logger!=null)
				logger.log(Level.INFO, "Try to log in");
		    HtmlPage page2 = (HtmlPage)form.getInputByValue("ログイン").click();
		    		    
		    webClient.waitForBackgroundJavaScript(120000);
		    
		    writer = new BufferedWriter(new FileWriter("c:\\after_login_buymapage.html"));
			writer.write(page2.asXml());		     
		    writer.close();
		    
		    if(logger!=null)
				logger.log(Level.FINE, "End buyMaLogin method");
		    return page2;
		}
		catch (Exception e) 
		{
            throw new Exception(e.getMessage());
        }
		finally 
    	{
    		if(webClient != null)
    		{
	            webClient.getCurrentWindow().getJobManager().removeAllJobs();
	            webClient.close();
	            System.gc();
    		}
        }
	}//buyMaLogin
	
	private WebClient StartWebBrowser() throws Exception
    {
    	try
    	{
	    	//light web browser start
			if(logger!=null)
				logger.log(Level.FINE, "Start the light Web Browser");
			final WebClient webClient = new WebClient(BrowserVersion.CHROME);
			webClient.getOptions().setUseInsecureSSL(true);
			webClient.getCookieManager().setCookiesEnabled(true);
	        webClient.getOptions().setJavaScriptEnabled(true);
	        webClient.getOptions().setTimeout(120000);
	        webClient.getOptions().setCssEnabled(true);
	        webClient.getOptions().setThrowExceptionOnScriptError(false);
	        webClient.getOptions().setPrintContentOnFailingStatusCode(false);
	        //webClient.setCssErrorHandler(new SilentCssErrorHandler());
	        //webClient.getOptions().setPrintContentOnFailingStatusCode(false);
	        webClient.getOptions().setPopupBlockerEnabled(true);
	        //webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
	        
	        /*
	        final History window = webClient.getWebWindows().get(0).getHistory();
	        final Field f = window.getClass().getDeclaredField("ignoreNewPages_"); //NoSuchFieldException
	        f.setAccessible(true);
	        ((ThreadLocal<Boolean>) f.get(window)).set(Boolean.TRUE);
	        
	        
	        String proxyname = "guess.proxy.eu";
	        int proxyport = 8080;
	        ProxyConfig proxy = new ProxyConfig(proxyname,proxyport);
	        webClient.getOptions().setProxyConfig(proxy);
	        
	        final DefaultCredentialsProvider cp = new DefaultCredentialsProvider();
	        cp.addCredentials("GUESSEU\tmorello", "password",proxyname,proxyport,null);
	        webClient.setCredentialsProvider(cp);
	        */
	        
	        return webClient;
    	}//try
        catch (Exception e) 
		{
            throw new Exception(e.getMessage());
        }
    }//StartWebBrowser
	 
	/**
     * Gets the html page from: the web main url concatenated to the one in the argument, and download it to text a file. 
     * @param url to be concatenated to the main web site link
     * @param FilepathFilename path and name of destination file
     * @throws Exception
     */
    public void getPageXml(String url, String FilepathFilename) throws Exception
    {
    	//open the web client
    	WebClient webClient = StartWebBrowser();
    	HtmlPage page = null;
    	String html = null;
    	BufferedWriter writer = null;
    	
    	try
    	{
    		page = webClient.getPage(webSiteLink+url);
			webClient.waitForBackgroundJavaScript(6000);
			html = page.asXml();
			writer = new BufferedWriter(new FileWriter(FilepathFilename));
		    writer.write(html);		     
		    writer.close();
    	}//try
    	catch (Exception e) 
		{
            throw new Exception(e.getMessage());
        }
    	finally 
    	{
    		if(webClient != null)
    		{
	            webClient.getCurrentWindow().getJobManager().removeAllJobs();
	            webClient.close();
	            writer.close();
	            System.gc();
    		}
        }//finally
    }//getPageXml
}//class
