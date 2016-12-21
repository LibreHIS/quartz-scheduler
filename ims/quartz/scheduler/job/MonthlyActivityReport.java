/*
 * Created on 19 Nov 2008
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package ims.quartz.scheduler.job;

import ims.configuration.gen.ConfigFlag;
import ims.core.admin.domain.objects.PrintAgent;
import ims.framework.utils.DateTime;
import ims.framework.utils.Time;
import ims.quartz.scheduler.helper.HashLink;
import ims.quartz.scheduler.hibernate.HibernateProxy;
import ims.quartz.scheduler.hibernate.HqlBatchUpdate;
import ims.quartz.scheduler.parser.csv.CsvParser;
import ims.quartz.scheduler.parser.csv.exceptions.CsvParserException;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.tree.DefaultElement;
import org.quartz.old.Job;
import org.quartz.old.JobDataMap;
import org.quartz.old.JobExecutionContext;
import org.quartz.old.JobExecutionException;

import com.ims.query.builder.client.QueryBuilderClient;
import com.ims.query.builder.client.SeedValue;
import com.ims.query.builder.client.exceptions.QueryBuilderClientException;

public class MonthlyActivityReport implements Job
{
	private static int	INDEX_TYPE		= 7;
	private static int	INDEX_ID		= 8;
	private static int	INDEX_SERVICE	= 9;
	
	private static final String	INTEGER			= "java.lang.Integer";
	private static final String	BIG_INTEGER		= "java.math.BigInteger";
	private static final String	SHORT			= "java.lang.Short";
	private static final String	LONG			= "java.lang.Long";
	private static final String	BOOOLEAN		= "java.lang.Boolean";
	private static final String	STRING			= "java.lang.String";
	private static final String	BIG_DECIMAL		= "java.math.BigDecimal";
	private static final String	FLOAT			= "java.lang.Float";
	private static final String	DOUBLE			= "java.lang.Double";
	private static final String	UTIL_DATE		= "java.util.Date";
	private static final String	SQL_DATE		= "java.sql.Date";
	private static final String	SQL_TIME		= "java.sql.Time";

	public static final String	HCP_BO			= "ims.core.resource.people.domain.objects.Hcp";
	private static final String	HCP_FIELD		= "Hcp_id";


	private static final String	HCP_LABEL		= "Select a Clinician";

	public static final String	GP_BO			= "ims.core.resource.people.domain.objects.Gp";
	private static final String	GP_FIELD		= "Gp_id";
	private static final String	GP_LABEL		= "Select a GP";

	public static final String	MEDIC_BO		= "ims.core.resource.people.domain.objects.Medic";
	private static final String	MEDIC_FIELD		= "Medic_id";
	private static final String	MEDIC_LABEL		= "Select a Medic";

	public static final String	MOS_BO			= "ims.core.resource.people.domain.objects.MemberOfStaff";
	private static final String	MOS_FIELD		= "MemberOfStaff_id";
	private static final String	MOS_LABEL		= "Select a Member of Staff";

	public static final String	ORG_BO			= "ims.core.resource.place.domain.objects.Organisation";
	private static final String	ORG_FIELD		= "Organisation_id";
	private static final String	ORG_LABEL		= "Select an Organisation";

	public static final String	LOCATION_BO		= "ims.core.resource.place.domain.objects.Location";
	private static final String	LOCATION_FIELD	= "Location_id";
	private static final String	LOCATION_LABEL	= "Select a Location";

	public static final String	LOC_SITE_BO		= "ims.core.resource.place.domain.objects.LocSite";
	private static final String	LOC_SITE_FIELD	= "LocSite_id";
	private static final String	LOC_SITE_LABEL	= "Select a Hospital";
	
	private static HashMap<String, HashLink> map = new HashMap<String, HashLink>();
	
	static
	{
		map.put("CR", new HashLink("CatsReferral", "uniqueLineRefNo"));
		map.put("CRS", new HashLink("CATSReferralStatus", "uniqueLineRefNo"));
		map.put("CRSR", new HashLink("CATSReferralStatus", "uniqueLineRefNoRejected"));
		map.put("REFDIS", new HashLink("DischargeOutcome", "uniqueLineRefNo"));
		map.put("CRSDI", new HashLink("CATSReferralStatus", "uniqueLineRefNoDI"));
		map.put("DISOUT", new HashLink("DischargeOutcome", "uniqueLineRefNoDI"));
		map.put("DISOUTMP", new HashLink("DischargeOutcome", "uniqueLineRefNoMP"));
		map.put("CRL1L3", new HashLink("ProviderCancellation", "uniqueLineRefNo"));
		map.put("CRSL1L3REJ", new HashLink("CATSReferralStatus", "uniqueLineRefNoL1"));
		map.put("APPSTAT", new HashLink("Appointment_Status", "uniqueLineRefNo"));
		map.put("PROC", new HashLink("PatientProcedure", "uniqueLineRefNo"));
	}
	
	public void execute(JobExecutionContext context) throws JobExecutionException
	{
		String queryServerUrl = ConfigFlag.GEN.QUERY_SERVER_URL.getValue();
		String reportServerUrl = ConfigFlag.GEN.REPORT_SERVER_URL.getValue();
		String seeds = null;
		
		JobDataMap map = context.getMergedJobDataMap();
		if(map != null)
		{
			seeds = (String) map.get("seeds");
		}
		
		//System.out.println(seeds);
		
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
		
		try
		{
			Date now = new Date();
			GregorianCalendar cal = new GregorianCalendar();
			cal.setTime(now);
			
			Integer year = new Integer(cal.get(Calendar.YEAR));
			Integer month = new Integer(cal.get(Calendar.MONTH) + 1);
			
			String fileName = "";
			SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
			String folderName = getOutputFolder();
			
			ArrayList<SeedValue> aSeeds = getSeeds(seeds, folderName + "MonthlyActivityReport_Error_" + df.format(now) + ".txt");
			
			if(aSeeds != null)
			{
				for (int i = 0; i < aSeeds.size(); i++)
				{
					if("YEAR".equalsIgnoreCase(aSeeds.get(i).getName()))
					{
						year = (Integer) aSeeds.get(i).getValue();
					}
					else if("MONTH".equalsIgnoreCase(aSeeds.get(i).getName()))
					{
						month = (Integer) aSeeds.get(i).getValue();
					}
				}
			}
			
			fileName = folderName + "MonthlyActivityReport_" + year + "_" + month + "_" + df.format(now) + ".csv"; 
			String fileNameErr = folderName + "MonthlyActivityReport_Error_" + year + "_" + month + "_" + df.format(now) + ".txt"; 
			
			System.out.println((now).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' got executed !!!");
			
			String query = "select r1_1.reportXml, t1_1.templateXml, r1_1.reportName, r1_1.reportDescription, t1_1.name, t1_1.description from ReportBo as r1_1 left join r1_1.templates as t1_1 where (r1_1.imsId= :imsid) order by t1_1.name";

			Integer id = new Integer(129);

			HibernateProxy hp = new HibernateProxy();
			
			ArrayList al = hp.find(query, new String[] {"imsid"}, new Object[] {id});
			
			for(int i = 0; i < al.size(); i++)
			{
				Object[] obj = (Object[])al.get(i);
		
				if(obj[0] != null && obj[1] != null)
				{
					QueryBuilderClient qb = new QueryBuilderClient(queryServerUrl, null);

					if(aSeeds.size() == 0)
					{
						qb.addSeed(new SeedValue("YEAR", year , Integer.class));
						qb.addSeed(new SeedValue("MONTH", month , Integer.class));
					}
					else
					{
						for (int j = 0; j < aSeeds.size(); j++)
						{
							qb.addSeed(aSeeds.get(j));
						}
					}
					
					try
					{
						byte[] doc = qb.buildReport((String)obj[0], (String)obj[1], reportServerUrl, QueryBuilderClient.CSV , "", 1);
						
						processCsvFile(now, doc, context, fileName, fileNameErr);
						
						//System.out.println(doc != null ? new String(doc) : null);
						
						Integer jobId = (Integer)context.getJobDetail().getJobDataMap().get("PrintAgentID");
						PrintAgent job = (PrintAgent)hp.getDomainObject(PrintAgent.class, jobId);
						
						if(job == null)
						{
							System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not retrieve the job from the database !!!");
							throw new JobExecutionException(" >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not retrieve the job from the database !!!");
						}
						
						job.setLastRunDateTime(now);
						if(doc != null && doc.length > 0)
							job.setLastSuccessfulRunDateTime(now);
		
						Object[] pojo = new Object[1];
						
						pojo[0] = job;
						
						boolean bRet = hp.saveOrUpdate(pojo);
						
						if(bRet)
							System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' executed succesfully !!!");
						else
							System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not update the job status in the database !!!");
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
			}
			
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			throw new JobExecutionException(e.toString());
		}
	}

	private void processCsvFile(Date now, byte[] doc, JobExecutionContext context, String fileName, String fileNameErr) throws JobExecutionException, IOException
	{
        String strLine;

        CsvParser parser = new CsvParser();

        ByteArrayInputStream fstream = new ByteArrayInputStream(doc);
        DataInputStream in = new DataInputStream(fstream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        
        String type;
        Integer id;
        Integer service;
        
        int lastService = 0;
        
        StringBuilder sbLines = new StringBuilder();
        ArrayList<HqlBatchUpdate> tran = new ArrayList<HqlBatchUpdate>();
        String uniqueLineRefNo = null;
        HibernateProxy hp = new HibernateProxy();

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

                if (tokens != null && tokens.length >= 10)
                {
                	type = tokens[INDEX_TYPE];
                	id = Integer.valueOf(tokens[INDEX_ID]);
                	service = Integer.valueOf(tokens[INDEX_SERVICE]);
                	
                	if(lastService != 0 && lastService != service.intValue())
                	{
                		//we commit the transaction for the current service

                		if(sbLines.length() > 0)
                			sbLines.append("\r\n");
                		
						commitTransaction(context, tran, hp, fileName, fileNameErr, sbLines);
                	}
                	
                	uniqueLineRefNo = getUniqueLineRefNo(hp);
                		
            		HqlBatchUpdate smt = new HqlBatchUpdate("update " + map.get(type).getBoName() + " t set t." + map.get(type).getFieldName() + " = :ULN where t.id = :ID", new String[] {"ULN", "ID"}, new Object[] {uniqueLineRefNo, id});
            		
            		tran.add(smt);
            		
            		if(sbLines.length() > 0)
            			sbLines.append("\r\n");
            		
            		sbLines.append(strLine.trim());
            		sbLines.append(", \"");
            		sbLines.append(uniqueLineRefNo);
            		sbLines.append("\"");
                	
            		lastService = service.intValue();
                }
            }
        }
        
        //stress test
        /*
        if(tran.size() > 0)
        {
        	int size = tran.size();
        	
        	System.out.println("Executing big transaction..." + size + " x " + 1000);

        	for (int k = 0; k < 1000; k++)
        	{
	        	for (int i = 0; i < size; i++)
				{
					tran.add(tran.get(i));
				}
        	}
        }
        */
        
		if(sbLines.length() > 0)
			sbLines.append("\r\n");
        
		commitTransaction(context, tran, hp, fileName, fileNameErr, sbLines);
	}

	private void commitTransaction(JobExecutionContext context, ArrayList<HqlBatchUpdate> tran, HibernateProxy hp, String fileName, String fileNameErr, StringBuilder sbLines)
	{
		if(tran.size() == 0)
			return;
		
		try
		{
			appendToFile(fileName, "");
		}
		catch (IOException e)
		{
			e.printStackTrace();
			return;
		}
		
		Object[] pojo = null;
		
		pojo = new Object[tran.size()];
		
		for (int i = 0; i < pojo.length; i++)
		{
			pojo[i] = tran.get(i);	
		}
		
		boolean bRet = hp.saveOrUpdate(pojo);
		
		if(bRet)
		{
			System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' commited succesfully " + pojo.length + " updates !");
			try
			{
				appendToFile(fileName, sbLines.toString());
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' failed to execute " + pojo.length + " updates !");
			try
			{
				appendToFile(fileNameErr, (new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' failed to execute " + pojo.length + " updates !\r\n");
				
				StringBuilder err = new StringBuilder();
				for (int i = 0; i < tran.size(); i++)
				{
					err.append(tran.get(i).getHqlUpdate());
					err.append("\r\n");
				}
				err.append("-------------------------------------------------\r\n");

				appendToFile(fileNameErr, err.toString());				
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		
		tran.clear();
		sbLines.setLength(0);
	}

	private String getUniqueLineRefNo(HibernateProxy hp)
	{
		ArrayList list = hp.findSql("exec LINEID");
		
		return list.get(0).toString();
	}

	private String paddingString(String s, int n, char c, boolean paddingLeft)
    {
        StringBuffer str = new StringBuffer(s);
        int strLength = str.length();
        if (n > 0 && n > strLength)
        {
            for (int i = 0; i <= n; i++)
            {
                if (paddingLeft)
                {
                    if (i < n - strLength)
                    {
                        str.insert(0, c);
                    }
                }
                else
                {
                    if (i > strLength)
                    {
                        str.append(c);
                    }
                }
            }
        }
        return str.toString();
    }	
	
	private String getOutputFolder() throws JobExecutionException
	{
		String folderName = null;
		
		if(ConfigFlag.GEN.BATCH_JOB_EXPORT_FOLDER.getValue() == null || ConfigFlag.GEN.BATCH_JOB_EXPORT_FOLDER.getValue().length() == 0)
		{
			if ( System.getProperty("catalina.home") == null)
			{
				throw new JobExecutionException("The JVM parameter 'catalina.home' was not found !");				
			}
			
			folderName = System.getProperty("catalina.home") + "/TransactionExport/";				
		}
		else
		{
			folderName = ConfigFlag.GEN.BATCH_JOB_EXPORT_FOLDER.getValue();
			
			if(!(folderName.endsWith("\\") || folderName.endsWith("/")))
			{
				folderName += System.getProperty("file.separator");
			}
		}

		File folder = new File(folderName);
		
		if(folder.exists() == false)
		{
			boolean bSuccess = folder.mkdir();
			
			if(bSuccess == false)
				throw new JobExecutionException("I could not create the folder '" + folder.getAbsolutePath() + "' !");
		}
		
		return folderName;
	}
	
	private void appendToFile(String fileName, String buffer) throws IOException
	{
		FileOutputStream fos = new FileOutputStream(fileName, true);
		fos.write(buffer.getBytes());
		fos.flush();
		fos.close();
	}

	@SuppressWarnings("deprecation")
	public Object getValue(String value, String dataType, String fileNameErr) throws IOException
	{
		if (value == null || value.length() == 0)
			return "";

		if (dataType == null || dataType.equals(""))
			return "";

		if (dataType.equals(INTEGER) || dataType.equals(BIG_INTEGER) || dataType.equals(SHORT) || dataType.equals(LONG))
		{
			try
			{
				if (dataType.equals(INTEGER))
				{
					return new Integer(Integer.parseInt(value));
				}
			}
			catch (NumberFormatException exc)
			{
				appendToFile(fileNameErr, (new Date()).toString() + " >> Failed to convert seed value for "  + value + " \r\n" + exc.toString() + " !\r\n");
			}
		}

		if (dataType.equals(BOOOLEAN))
		{
			return Boolean.valueOf(value);
		}

		if (dataType.equals(STRING))
		{
			return value;
		}

		if (dataType.equals(BIG_DECIMAL) || dataType.equals(FLOAT) || dataType.equals(DOUBLE))
		{
			return Float.valueOf(value);
		}

		if (dataType.equals(SQL_DATE))
		{
			return new Date(value).getDate();
		}

		if (dataType.equals(UTIL_DATE))
		{
			try
			{
				return new DateTime(value).getJavaDate();
			}
			catch (ParseException e)
			{
				appendToFile(fileNameErr, (new Date()).toString() + " >> Failed to convert seed value for "  + value + " \r\n" + e.toString() + " !\r\n");
			}
		}
		
		if (dataType.equals(SQL_TIME))
		{
			return new Time(value);
		}

		return "";
	}
	
	ArrayList<SeedValue> getSeeds(String xmlSeeds, String fileNameErr) throws DocumentException, IOException, ClassNotFoundException
	{
		ArrayList<SeedValue> seeds = new ArrayList<SeedValue>();
		
		if(xmlSeeds != null && xmlSeeds.length() > 0)
		{
			Document document = DocumentHelper.parseText(xmlSeeds);

			List list = document.selectNodes("//seeds/seed");
			for (Iterator iter = list.iterator(); iter.hasNext();)
			{
				DefaultElement attribute = (DefaultElement) iter.next();

				Object val = getValue(attribute.valueOf("v"), attribute.valueOf("t"), fileNameErr);
				
				SeedValue sv = new SeedValue(attribute.valueOf("n"), val, Class.forName(attribute.valueOf("t")));
				
				seeds.add(sv);
			}
			
		}
		
		return seeds;
	}
	
}
