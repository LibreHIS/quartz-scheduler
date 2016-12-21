package ims.quartz.scheduler.job;

import ims.configuration.gen.ConfigFlag;
import ims.core.admin.domain.objects.PrintAgent;
import ims.ocrr.orderingresults.domain.objects.CentralBatchPrint;
import ims.quartz.scheduler.hibernate.HibernateProxy;
import ims.quartz.scheduler.hibernate.HibernateUtil3;
import ims.quartz.scheduler.parser.csv.CsvParser;
import ims.quartz.scheduler.parser.csv.exceptions.CsvParserException;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.quartz.old.Job;
import org.quartz.old.JobExecutionContext;
import org.quartz.old.JobExecutionException;

import com.ims.query.builder.client.QueryBuilderClient;
import com.ims.query.builder.client.SeedValue;
import com.ims.query.builder.client.exceptions.QueryBuilderClientException;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.pdf.PdfCopyFields;
import com.itextpdf.text.pdf.PdfReader;

public class CentralBatchPrintJob implements Job
{
	private static int	INDEX_RESULTS_TO_PRINTED		= 0;
	private static int	INDEX_ORDER_ID		= 1;
	private static int	INDEX_CONSULTANT_NAME	= 2;
	private static int	INDEX_CONSULTANT_ID	= 3;
	
	private static int	RESULT_DETAILS_BY_CONSULTANT = 241;
	private static int	RESULT_DETAILS_COVER_SHEET = 242;
	private static int	RESULT_DETAILS_SUMMARY = 245;
	
	//private static Boolean isBusyId = false;
	//private static Boolean isBusyCoverSheet = false;
	//private static Boolean isBusyConsultant = false;
	//private static Boolean isBusySummary = false;
	
	private Date startJobTime;
	private Date endJobTime;
	private long maxTimeToBuildReport;
	private long maxTimeToConcatenate;
	private int numberOfResultsProcessed;
	
	@SuppressWarnings("unchecked")
	public void execute(JobExecutionContext context) throws JobExecutionException 
	{
		initializeLogVariables();
		
		String queryServerUrl = ConfigFlag.GEN.QUERY_SERVER_URL.getValue();
		String reportServerUrl = ConfigFlag.GEN.REPORT_SERVER_URL.getValue();
		
		if(queryServerUrl == null || queryServerUrl.length() == 0 || queryServerUrl.equals(ConfigFlag.GEN.QUERY_SERVER_URL.getDefaultValue()))
		{
			System.out.println("The config flag QUERY_SERVER_URL was not set !");
			throw new JobExecutionException("The config flag QUERY_SERVER_URL was not set !");
		}
		
		if(reportServerUrl == null || reportServerUrl.length() == 0 || reportServerUrl.equals(ConfigFlag.GEN.REPORT_SERVER_URL.getDefaultValue()))
		{
			System.out.println("The config flag REPORT_SERVER_URL was not set !");
			throw new JobExecutionException("The config flag REPORT_SERVER_URL was not set !");
		}
		
		System.out.println((startJobTime = new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' got executed !!!");
		
		HibernateProxy hp = new HibernateProxy();
		
		String query = "select p1_1.id, p1_1.queryServerUrl, p1_1.reportServerUrl, t1_1.template.id, t1_1.printerName, t2_1.report.id, t2_1.templateXml, r1_1.reportXml from PrintAgent as p1_1 left join p1_1.templatePrinters as t1_1 left join t1_1.template as t2_1 left join t2_1.report as r1_1 where (p1_1.id = :ID)";
		Integer idJob = (Integer)context.getJobDetail().getJobDataMap().get("PrintAgentID");
		List<Object> al = hp.find(query, new String[] {"ID"}, new Object[] {idJob});
		
		PrintAgent job = (PrintAgent)hp.getDomainObject(PrintAgent.class, idJob);
		if(job == null)
		{
			System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not retrieve the job from the database !!!");
			throw new JobExecutionException(" >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not retrieve the job from the database !!!");
		}
		
		Object[] resultDetailsCoverSheetReport = getSystemReportAndTemplate(hp, RESULT_DETAILS_COVER_SHEET);
		Object[] resultDetailsByConsultant = getSystemReportAndTemplate(hp, RESULT_DETAILS_BY_CONSULTANT);
		
		for(int i = 0; i < al.size(); i++)
		{
			Object[] obj = (Object[])al.get(i);
			
			String repXml = (String) obj[7];
			String templXml = (String) obj[6];
			String printerName = (String) obj[4];
			
			if(templXml != null && repXml != null)
			{
				QueryBuilderClient qb = new QueryBuilderClient(queryServerUrl, null);
				
				List<ResultDetailsID> resultsList = processResultsDetailsIdReport(qb, reportServerUrl, repXml, templXml, job);
				
				if(resultsList != null && resultsList.size() > 0)
				{
					System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' got the list of IDs (" + resultsList.size() + (resultsList.size() == 1 ? " result" : " results") + ")");
					
					printReports(hp, qb, reportServerUrl, resultDetailsCoverSheetReport, resultDetailsByConsultant, printerName, resultsList, job);
					saveJob(job, hp);
				}
			}
		}
		endJobTime = new Date();
		
		displaySummaryLog(job);
	}

	private void initializeLogVariables() 
	{
		startJobTime = null;
		endJobTime = null;
		maxTimeToBuildReport = 0;
		maxTimeToConcatenate = 0;
		numberOfResultsProcessed = 0;
	}
	
	private void displaySummaryLog(PrintAgent job) 
	{
		if(job == null)
			return;
		
		StringBuilder sb = new StringBuilder();
		sb.append("-----------------------------------------------------------------------------------------");
		sb.append("\n|  The job \'" + job.getDescription() + "\' started at " + (startJobTime != null ? startJobTime.toString() : ""));
		sb.append("\n|  The job \'" + job.getDescription() + "\' ended at " + (endJobTime != null ? endJobTime.toString() : ""));
		
		if(startJobTime != null && endJobTime != null)
			sb.append("\n|  The job \'" + job.getDescription() + "\' took " + getElapsedTimeHoursMinutesSecondsString(endJobTime.getTime() - startJobTime.getTime()) + " to execute");
		
		sb.append("\n|  The job \'" + job.getDescription() + "\' processed " + numberOfResultsProcessed + (numberOfResultsProcessed == 1 ? " result" : " results"));
		sb.append("\n|  Longest build time for \'Result Details by Consultant\' report was " + getElapsedTimeHoursMinutesSecondsString(maxTimeToBuildReport));
		sb.append("\n|  Longest time to concatenate a result to the final PDF file was " + getElapsedTimeHoursMinutesSecondsString(maxTimeToConcatenate));
		sb.append("\n-----------------------------------------------------------------------------------------");
		
		System.out.println(sb.toString());
	}
	
	public String getElapsedTimeHoursMinutesSecondsString(long elapsedTime) 
	{ 
		String format = String.format("%%0%dd", 2);  
		String miliseconds = 
		String.format(String.format("%%0%dd", 3), elapsedTime % 1000); 
		elapsedTime = elapsedTime / 1000;  
		String seconds = String.format(format, elapsedTime % 60);  
		String minutes = String.format(format, (elapsedTime % 3600) / 60);  
		String hours = String.format(format, elapsedTime / 3600);  
		String time =  hours + ":" + minutes + ":" + seconds + ":" + miliseconds;  
		
		return time;  
	}  

	private void saveJob(PrintAgent job, HibernateProxy hp) throws JobExecutionException 
	{
		if(job == null)
			return;
		
		Transaction tx = null;
		Session session = null;

		try
		{
			session = HibernateUtil3.currentSession();
			tx = session.beginTransaction();
		}
		catch (HibernateException e1)
		{
			e1.printStackTrace();
		}

		try
		{
			job = (PrintAgent) session.get(PrintAgent.class, job.getId());
			
			Date now = new Date();
			job.setLastRunDateTime(now);
			job.setLastSuccessfulRunDateTime(now);
			
			session.saveOrUpdate(job);
			tx.commit();
		}
		catch(HibernateException e)
		{
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
		}
		finally
		{
			try
			{
				HibernateUtil3.closeSession();
			}
			catch (HibernateException e2)
			{
				throw new JobExecutionException(e2.toString());
			}
		}
	}

	private void printReports(HibernateProxy hp, QueryBuilderClient qb, String reportServerUrl, Object[] resultDetailsCoverSheetReport, Object[] resultDetailsByConsultant, String printerName, List<ResultDetailsID> resultsList, PrintAgent job) throws JobExecutionException 
	{
		java.util.Date date = new java.util.Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		String timestamp = df.format(date);
		
		String fileName = "ResultCentralBatchPrint_" + job.getId() + "_" + timestamp + ".pdf";	
		String resultDetailsCoverSheetReportXML = (String) resultDetailsCoverSheetReport[0];
		String resultDetailsCoverSheetTemplateXML = (String) resultDetailsCoverSheetReport[1];
		String resultDetailsByConsultantReportXML = (String) resultDetailsByConsultant[0];
		String resultDetailsByConsultantTemplateXML = (String) resultDetailsByConsultant[1];
		
		Integer lastConsultantId = null;
		Integer numberOfResultsPerConsultant = null;
		
		HashMap<Integer, Integer> noOfResultsPerConsultant = getNoPerConsultant(resultsList);
		LinkedHashMap<Integer, Integer> noOfPagesPerConsultant = new LinkedHashMap<Integer, Integer>();
		
		System.out.println((new Date()).toString() + " >> The job '" +job.getDescription() + "' begins processing the results !!!");
		
		for(int i=0; i<resultsList.size(); i++)
		{
			ResultDetailsID result = resultsList.get(i);
			
			if(lastConsultantId == null || !lastConsultantId.equals(result.getConsultantId()))
			{
				lastConsultantId = result.getConsultantId();
				numberOfResultsPerConsultant = 1;
				
				byte[] pdfCover = printCoverSheet(qb, reportServerUrl, resultDetailsCoverSheetReportXML, resultDetailsCoverSheetTemplateXML, printerName, result);
				concatenateToPdf(fileName, pdfCover, job, lastConsultantId, noOfPagesPerConsultant);
			}
			
			byte[] pdfresultDetails = printResultDetailsByConsultant(qb, reportServerUrl, resultDetailsByConsultantReportXML, resultDetailsByConsultantTemplateXML, printerName, result, numberOfResultsPerConsultant, noOfResultsPerConsultant.get(result.getConsultantId()));
			long beforeConcatenate = System.currentTimeMillis();
			concatenateToPdf(fileName, pdfresultDetails, job, lastConsultantId, noOfPagesPerConsultant);
			long afterConcatenate = System.currentTimeMillis();
			
			long maxTimeToConcatenateTemp = afterConcatenate - beforeConcatenate;
			if(maxTimeToConcatenateTemp > maxTimeToConcatenate)
				maxTimeToConcatenate = maxTimeToConcatenateTemp;
			
			deleteFromCentralBatchPrint(hp, result);
			
			numberOfResultsPerConsultant++;
			
			numberOfResultsProcessed++;
		}
		
		System.out.println((new Date()).toString() + " >> The job '" +job.getDescription() + "' ends processing the results !!!");
		
		int totalPages = getPages(fileName);
		byte[] pdfSummary = printResultDetailsSummary(qb, reportServerUrl, hp, printerName, resultsList, noOfPagesPerConsultant, totalPages);
		concatenateFiles(fileName, pdfSummary, job);
		
		//get location's Pathology System code (WDEV-11560)
		String code = null;
		
		if(job.getLocationToPrintFor() != null)
			code = getSiteCode(hp, job.getLocationToPrintFor().getId());
		
		String fullFileName = uploadResultsDetailsPdfToServer(fileName, code);
		
		//read previously unprinted file contents and attempt to print	
		byte[] pdfArray = readFileIntoByteArray(fullFileName);
		if(pdfArray != null)
		{
			try
			{
				qb.printReport(pdfArray, reportServerUrl, printerName, 1);
			}
			catch (QueryBuilderClientException e)
			{
				throw new JobExecutionException("Printing " + fileName +  " failed when attempting to print - " + e.getMessage());
			}
		}
	}

	//read the contents of the concatenated pdf file into a byte array so we can print
	private byte[] readFileIntoByteArray(String fileName) throws JobExecutionException 
	{
		File file = new File(fileName);
		if(file.exists())
		{
			try
			{	
				FileInputStream fin = new FileInputStream(file);
				
				byte fileContent[] = new byte[(int)file.length()];
				
				fin.read(fileContent);
				
				return fileContent;
			}
			catch(FileNotFoundException e)
			{      
				throw new JobExecutionException("Printing " + fileName +  " failed when attempting to print - " + e.getMessage());
			}    
			catch(IOException ioe)    
			{      
				throw new JobExecutionException("Printing " + fileName +  " failed when attempting to print - " + ioe.getMessage());
			}
		}
		return null;
	}

	private byte[] printResultDetailsSummary(QueryBuilderClient qb, String reportServerUrl, HibernateProxy hp, String printerName, List<ResultDetailsID> resultsList, LinkedHashMap<Integer, Integer> noOfPagesPerConsultant, int totalPages) throws JobExecutionException 
	{
		Object[] resultDetailsSummary = getSystemReportAndTemplate(hp, RESULT_DETAILS_SUMMARY);
		
		String resultDetailsSummaryReportXML = (String) resultDetailsSummary[0];
		String resultDetailsSummaryTemplateXML = (String) resultDetailsSummary[1];
		
		qb.getSeeds().clear();
		
		qb.addSeed(new SeedValue("TOTALPAGES", totalPages , Integer.class));
		
		Iterator<Integer> iterator = noOfPagesPerConsultant.keySet().iterator();

		while(iterator.hasNext())
		{
			Integer consultantId = iterator.next();
			
			qb.addSeed(new SeedValue("CONSULTANT", getConsultantName(consultantId, resultsList), String.class));
			qb.addSeed(new SeedValue("PAGES", noOfPagesPerConsultant.get(consultantId), Integer.class));
		}
		
		try 
		{
			//synchronized(isBusySummary)
			//{
				return qb.buildReport(resultDetailsSummaryReportXML, resultDetailsSummaryTemplateXML, reportServerUrl, QueryBuilderClient.PDF , "", 1);
			//}
		} 
		catch (QueryBuilderClientException e) 
		{
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
		}
	}

	private String getConsultantName(Integer consultantId, List<ResultDetailsID> resultsList) 
	{
		if(consultantId == null || resultsList == null || resultsList.size() == 0)
			return null;
		
		for(int i=0; i<resultsList.size(); i++)
		{
			if(resultsList.get(i).getConsultantId().equals(consultantId))
				return resultsList.get(i).getConsultantName();
		}
		
		return null;
	}

	private HashMap<Integer, Integer> getNoPerConsultant(List<ResultDetailsID> resultsList) 
	{
		HashMap<Integer, Integer> tempResult = new HashMap<Integer, Integer>();
		
		for(int i=0; i<resultsList.size(); i++)
		{
			Integer aux = 1;
			
			if(tempResult.containsKey(resultsList.get(i).getConsultantId()))
				aux = tempResult.get(resultsList.get(i).getConsultantId()) + 1;
			
			tempResult.put(resultsList.get(i).getConsultantId(), aux);
		}
		
		return tempResult;
	}

	private String uploadResultsDetailsPdfToServer(String fileName, String code) throws JobExecutionException 
	{
		String storePath = ConfigFlag.GEN.PDF_STORE_PATH.getValue();

		if(!(storePath.endsWith("/") || storePath.endsWith("\\")))
			storePath = storePath + "/";

		java.util.Date date = new java.util.Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyy/M/d");
		String ymd = df.format(date);
		
		String filePath = storePath + ymd + "/";

		System.out.println("Store path : " + filePath);
		File newDir = new File(filePath);

		if (!newDir.exists()) 
		{
			try
			{
				newDir.mkdirs();
				System.out.println("Succesfully created new directory: " + newDir.getPath());
			}
			catch(Exception e)
			{
				throw new JobExecutionException(e.toString());
			}
		}
		
		String fullFileName;
		
		if(code != null)
			fullFileName = filePath + code + fileName;
		else
			fullFileName = filePath + fileName;

		System.out.println("Trying to upload report to: " + fullFileName);
		
        copyFileToServer(fileName, fullFileName);
        
        deleteFile(new File(fileName));
        
        System.out.println("Successfully upload report to: " + fullFileName);
        
        return fullFileName;
	}

	private void copyFileToServer(String fromFile, String toFile) throws JobExecutionException 
	{
		FileInputStream from = null;
	    FileOutputStream to = null;
	    try 
	    {
			from = new FileInputStream(fromFile);
			to = new FileOutputStream(toFile);
			byte[] buffer = new byte[1024 * 64];
		    int bytesRead;

			while ((bytesRead = from.read(buffer)) != -1)
				to.write(buffer, 0, bytesRead);
	    } 
	    catch (FileNotFoundException e) 
	    {
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
	    }
	    catch (IOException e) 
	    {
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
	    } 
	    finally 
	    {
	    	if (from != null)
	        try 
	      	{
	        	from.close();
	        } 
	    	catch (IOException e) 
	    	{
	    		throw new JobExecutionException(e.toString());
	        }
	    	if (to != null)
	    	{
	    		try 
	    		{
	    			to.close();
	    		} 
	    		catch (IOException e) 
	    		{
	    			throw new JobExecutionException(e.toString());
	    		}
	    	}
	    }
	}

	private void deleteFromCentralBatchPrint(HibernateProxy hp, ResultDetailsID result) 
	{		
		//WDEV-11601 
		try
		{
			CentralBatchPrint recordtoBeDeteled = (CentralBatchPrint) hp.getDomainObject(CentralBatchPrint.class, result.getResultToBePrintedId());
			hp.delete(recordtoBeDeteled);
		}
		catch (HibernateException ex)
		{
			//do nothing here - record possibly deleted by another job
		}
	}

	private void concatenateToPdf(String fileName, byte[] pdf, PrintAgent job, Integer consultantId, LinkedHashMap<Integer, Integer> noOfPagesPerConsultant) throws JobExecutionException
	{
		File finalPdfFile = new File(fileName);
		
		if(!finalPdfFile.exists())
		{
			writeToFile(fileName, pdf);
			int noOfPages = getPages(fileName);
			
			if(noOfPagesPerConsultant.containsKey(consultantId))
				noOfPages = noOfPagesPerConsultant.get(consultantId) + noOfPages;
			
			noOfPagesPerConsultant.put(consultantId, noOfPages);
			
		}
		else
		{    
			int noOfPages = concatenateFiles(fileName, pdf, job);
			
			if(noOfPagesPerConsultant.containsKey(consultantId))
				noOfPages = noOfPagesPerConsultant.get(consultantId) + noOfPages;
			
			noOfPagesPerConsultant.put(consultantId, noOfPages);
		}
	}

	private int concatenateFiles(String fileName, byte[] pdf, PrintAgent job) throws JobExecutionException 
	{
		java.util.Date date = new java.util.Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		String timestamp = df.format(date);
		
		String tempPdfFile = "ResultDetails_" + timestamp + job.getId() + "_Temp.pdf";
		int numberOfPages = 0;
		
		File tempFile = new File(tempPdfFile);
		
		if(tempFile.exists())
		{
			deleteFile(tempFile);
		}
		
		writeToFile(tempPdfFile, pdf);
		
		File file = new File(fileName);
		File fileNew = new File("_" + fileName);

        if(fileNew.exists())
        {
            deleteFile(fileNew);
        }

        renameFile(file, fileNew);
        
		try 
		{
			PdfReader reader1 = new PdfReader(fileNew.getName());
		    PdfReader reader2 = new PdfReader(tempPdfFile);
		    
		    numberOfPages = reader2.getNumberOfPages();
		    
		    PdfCopyFields copy = new PdfCopyFields(new FileOutputStream(fileName));
		    copy.addDocument(reader1);
		    copy.addDocument(reader2);
		    copy.close();
		    
		    deleteFile(fileNew);
		    deleteFile(tempFile);

		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
		} 
		catch (DocumentException e) 
		{
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
		}
		catch (IOException e)  
		{
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
		}
		
		return numberOfPages;
	}

	private void renameFile(File file, File fileNew) throws JobExecutionException 
	{
		boolean success = false;
		success = file.renameTo(fileNew);
		
		if (!success)
		{
		    System.out.println("File '" + file.getName() + "' was not successfully renamed to '" + fileNew + "' !");
		    throw new JobExecutionException("File '" + file.getName() + "' was not successfully renamed to '" + fileNew + "' !");
		}
	}

	private void deleteFile(File tempFile) throws JobExecutionException 
	{
		boolean success = false;
		
		success = tempFile.delete();

		if (!success)
		{
		    System.out.println("File '" + tempFile + "' was not successfully deleted !");
		    throw new JobExecutionException("File '" + tempFile + "' was not successfully deleted !");
		}
	}

	private void writeToFile(String fileName, byte[] pdf) throws JobExecutionException 
	{
		FileOutputStream fos;
		
		try 
		{
			fos = new FileOutputStream(fileName);
			fos.write(pdf);
			fos.close();
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
		}
	}

	private byte[] printResultDetailsByConsultant(QueryBuilderClient qb, String reportServerUrl, String resultDetailsByConsultantReportXML,	String resultDetailsByConsultantTemplateXML, String printerName, ResultDetailsID result, int currentResult, Integer totalResults) throws JobExecutionException 
	{
		qb.getSeeds().clear();
		
		qb.addSeed(new SeedValue("OrderInvestigation_id", result.getOrderInvestigationId() , Integer.class));
		qb.addSeed(new SeedValue("FOOTER", currentResult + " of " + totalResults, String.class));
		
		try 
		{
			//synchronized(isBusyConsultant)
			//{
				long beforeBuild = System.currentTimeMillis();
				byte[] resultDeatilsByConsultant = qb.buildReport(resultDetailsByConsultantReportXML, resultDetailsByConsultantTemplateXML, reportServerUrl, QueryBuilderClient.PDF , "", 1);
				long afterBuild = System.currentTimeMillis();
				
				long maxTimeToBuildReportTemp = afterBuild - beforeBuild;
				
				if(maxTimeToBuildReportTemp > maxTimeToBuildReport)
					maxTimeToBuildReport = maxTimeToBuildReportTemp;
				
				return resultDeatilsByConsultant;
			//}
		} 
		catch (QueryBuilderClientException e) 
		{
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
		}
	}

	private byte[] printCoverSheet(QueryBuilderClient qb, String reportServerUrl, String resultDetailsCoverSheetReportXML, String resultDetailsCoverSheetTemplateXML, String printerName, ResultDetailsID result) throws JobExecutionException 
	{
		qb.getSeeds().clear();
		
		qb.addSeed(new SeedValue("OrderInvestigation_id", result.getOrderInvestigationId() , Integer.class));
		
		try 
		{
			//synchronized(isBusyCoverSheet)
			//{		
				return qb.buildReport(resultDetailsCoverSheetReportXML, resultDetailsCoverSheetTemplateXML, reportServerUrl, QueryBuilderClient.PDF , "", 1);		
			//}
		} 
		catch (QueryBuilderClientException e) 
		{
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
		}
	}

	@SuppressWarnings("unchecked")
	private Object[] getSystemReportAndTemplate(HibernateProxy hp, int reportImsId) 
	{
		String query = "select r1_1.reportXml, t1_1.templateXml from ReportBo as r1_1 left join r1_1.templates as t1_1 where r1_1.imsId = " + reportImsId;
		
		List<Object> al = hp.find(query);
		
		for(int i = 0; i < al.size(); i++)
		{
			if(al.get(i) != null)
				return (Object[])al.get(i);
		}
			
		return null;
	}

	private List<ResultDetailsID> processResultsDetailsIdReport(QueryBuilderClient qb,	String reportServerUrl, String repXml, String templXml, PrintAgent job) throws JobExecutionException 
	{
		qb.getSeeds().clear();
		qb.addSeed(new SeedValue("LocSite_id", job.getLocationToPrintFor().getId() , Integer.class));
		
		try
		{
			byte[] doc = null;
			//synchronized(isBusyId)
			//{
				doc = qb.buildReport(repXml, templXml, reportServerUrl, QueryBuilderClient.CSV , "", 1);
			//}
			
			return processCsvFile(doc);
		} 
		catch (QueryBuilderClientException e)
		{
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
		}
	}

	private List<ResultDetailsID> processCsvFile(byte[] doc) throws JobExecutionException, IOException
	{
		List<ResultDetailsID> resultsList = new ArrayList<ResultDetailsID>();
	
		String strLine;
        CsvParser parser = new CsvParser();
        ByteArrayInputStream fstream = new ByteArrayInputStream(doc);
        DataInputStream in = new DataInputStream(fstream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        
        // Read File Line By Line
        while ((strLine = br.readLine()) != null)
        {
            // Process current line from file
            if (strLine != null && strLine.trim().length() > 0)
            {
                String[] tokens = null;
                try
                {
                    tokens = parser.parseLine(strLine.trim());
                }
                catch (CsvParserException ex)
                {
                	throw new JobExecutionException(ex);
                }

                if (tokens != null && tokens.length == 4)
                {
                	Integer resultId = Integer.valueOf(tokens[INDEX_RESULTS_TO_PRINTED]);
                	Integer orderId = Integer.valueOf(tokens[INDEX_ORDER_ID]);
                	String consultant = tokens[INDEX_CONSULTANT_NAME];
                	Integer consultantId = Integer.valueOf(tokens[INDEX_CONSULTANT_ID]);
                	
                	resultsList.add(new ResultDetailsID(resultId, orderId, consultant, consultantId));
                }
            }
        }
        
        return resultsList;
	}
	
	private int getPages(String fileName) throws JobExecutionException
    { 
        PdfReader reader = null; 
         
        int numPages = 0; 

        try 
        { 
        	reader = new PdfReader(fileName);
            numPages = reader.getNumberOfPages(); 
        } 
        catch (IOException e) 
        {
        	e.printStackTrace();
        	throw new JobExecutionException(e.toString());
        } 
        finally 
        { 
            reader.close(); 
        } 

        return numPages; 
    }  
	
	@SuppressWarnings("unchecked")
	private String getSiteCode(HibernateProxy hp, Integer locId)
	{
		if(locId != null)
		{
			System.out.println((new Date()).toString() + " >> Getting site code for location " + locId + "...");
			
			String query = "select tax.taxonomyCode from LocSite as ls left join ls.codeMappings as tax left join tax.taxonomyName as lkp where (lkp.id = -822 and ls.id = :LocSite_id)";
	
			List<Object> al = hp.find(query, new String[] {"LocSite_id"}, new Object[] {locId});
			
			for(int i = 0; i < al.size(); i++)
			{
				if(al.get(i) != null)
				{
					System.out.println((new Date()).toString() + " >> Site code found for location " + locId + ": " + (String)al.get(i));
					return (String)al.get(i);
				}
				else
				{
					System.out.println((new Date()).toString() + " >> No site code found for location " + locId + " !");
				}
			}
			
			if(al.size() == 0)
				System.out.println((new Date()).toString() + " >> No site code found for location " + locId + " !");
		}
			
		return null;		
	}

	private class ResultDetailsID
	{
		 private Integer resultToBePrintedId;
		 private Integer orderInvestigationId;
		 private String consultantName;
		 private Integer consultantId;
		 
		 public ResultDetailsID(Integer resultId, Integer orderId, String consultant, Integer consultantID)
		 {
			 resultToBePrintedId = resultId;
			 orderInvestigationId = orderId;
			 consultantName = consultant;
			 consultantId = consultantID;
		 }
		 
		 public Integer getResultToBePrintedId()
		 {
			 return resultToBePrintedId;
		 }
		 
		 public Integer getOrderInvestigationId()
		 {
			 return orderInvestigationId;
		 }
		 
		 public String getConsultantName()
		 {
			 return consultantName;
		 }
		 
		 public Integer getConsultantId()
		 {
			 return consultantId;
		 }
	}
}
