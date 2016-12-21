/*
 * Created on 11-Aug-2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package ims.quartz.scheduler.job;

import ims.configuration.gen.ConfigFlag;
import ims.core.admin.domain.objects.PrintAgent;
import ims.core.resource.domain.objects.ServiceActivityExport;
import ims.quartz.scheduler.hibernate.HibernateProxy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.quartz.old.Job;
import org.quartz.old.JobExecutionContext;
import org.quartz.old.JobExecutionException;

import com.ims.query.builder.client.QueryBuilderClient;
import com.ims.query.builder.client.exceptions.QueryBuilderClientException;

public class ProcedureSpecialtyLinkRecords implements Job
{

	public void execute(JobExecutionContext context) throws JobExecutionException
	{
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
		
		boolean csvFileCreated = false;
		String fileName = "";
		HibernateProxy hp = null;
		PrintAgent job = null;
		Date now = new Date();
		
		try
		{
			System.out.println((now).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' got executed !!!");
			
			SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
			String folderName = getOutputFolder();
			fileName = folderName + "PSLR_" + df.format(now) + ".csv"; 
			String fileNameErr = folderName + "PSLRError_" + df.format(now) + ".txt";
			System.out.println(fileName);
			
			String query = "select p1_1.id, p1_1.queryServerUrl, p1_1.reportServerUrl, t1_1.template.id, t1_1.printerName, t2_1.report.id, t2_1.templateXml, r1_1.reportXml\r\n" + 
			"from PrintAgent as p1_1 left join p1_1.templatePrinters as t1_1 left join t1_1.template as t2_1 left join t2_1.report as r1_1\r\n" + 
			"where \r\n" + 
			"(p1_1.id = :ID) ";

			Integer id = (Integer)context.getJobDetail().getJobDataMap().get("PrintAgentID");

			hp = new HibernateProxy();
			job = (PrintAgent)hp.getDomainObject(PrintAgent.class, id);

			if(job == null)
			{
				System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not retrieve the job from the database !!!");
				throw new JobExecutionException(" >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not retrieve the job from the database !!!");
			}
			
			ArrayList al = hp.find(query, new String[] {"ID"}, new Object[] {id});
			/*
			ArrayList ids = hp.find("select distinct s1_1.id\r\n" + 
					"from ServiceActivity as s1_1 left join s1_1.service as s2_1 left join s2_1.taxonomyMap as t1_1 left join t1_1.taxonomyName as l1_1 left join s1_1.activity as a1_1 left join s1_1.taxonomyMap as t2_1 left join t2_1.taxonomyName as l2_1\r\n" + 
					"where \r\n" + 
					"(s1_1.id not in (select s1_1.serviceActivity.id\r\n" + 
					"from ServiceActivityExport as s1_1)\r\n" + 
					" and  (l1_1.id = -513 and l2_1.id = -513 ) \r\n" + 
					") \r\n" + 
					" and s1_1.isRIE is null ");
			*/
			
			for(int i = 0; i < al.size(); i++)
			{
				Object[] obj = (Object[])al.get(i);
		
				if(obj[7] != null && obj[6] != null && obj[2] != null)
				{
					QueryBuilderClient qb = new QueryBuilderClient(queryServerUrl, null);
					
					try
					{
						byte[] doc = qb.buildReport((String)obj[7], (String)obj[6], reportServerUrl, QueryBuilderClient.CSV , "", 1);
						boolean bRet = (doc != null ? true : false);
						
						if(bRet == false)
						{
							System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not build the report !!!");
							throw new JobExecutionException(" >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not build the report !!!");
						}
						
						ArrayList ids = new ArrayList();
						byte[] newDoc = processExportFile(ids, doc, fileNameErr);
						
						FileOutputStream fos = new FileOutputStream(fileName);

						csvFileCreated = true;

						boolean error = false;
						try
						{
							fos.write(newDoc);
						}
						catch(Exception e)
						{
							error = true;
							fos.flush();
							fos.close();
							
							throw e;
						}
						finally
						{
							if(error == false)
							{
								fos.flush();
								fos.close();
							}
						}
						
						job.setLastRunDateTime(now);
						if(bRet)
							job.setLastSuccessfulRunDateTime(now);
		
						Object[] pojo = new Object[1 + ids.size()];
						
						pojo[0] = job;
						
						for (int j = 0; j < ids.size(); j++)
						{
							ArrayList list = hp.find("select s from ServiceActivityExport s where s.serviceActivity.id = :ID", new String[] {"ID"}, new Object[] {ids.get(j)});
							
							if(list == null || list.size() == 0)
								throw new RuntimeException("Could not retrieve ServiceActivityExport for ServiceActivity_id = " + id + " !");

							if(list.size() > 1)
								throw new RuntimeException("More than one ServiceActivityExport found for ServiceActivity_id = " + id + " !");
							
							ServiceActivityExport sae = (ServiceActivityExport) list.get(0);
							
							//sae.setServiceActivity((ServiceActivity) hp.getDomainObject(ServiceActivity.class, ((Integer)ids.get(j)).intValue()));
							//sae.getSystemInformation().setCreationDateTime(now);
							//sae.getSystemInformation().setLastUpdateDateTime(now);
							//sae.setExported(Boolean.TRUE);
							sae.setDateLastExported(now);
							sae.setReadyForExport(Boolean.FALSE);
							
							pojo[1+j] = sae;
						}
						
						bRet = hp.saveOrUpdate(pojo);
						
						if(bRet)
							System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' executed succesfully !!!");
						else
						{
							System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not update the job status in the database !!!");
							deleteFile(fileName, csvFileCreated);
						}
					}
					catch (QueryBuilderClientException e)
					{
						deleteFile(fileName, csvFileCreated);
						e.printStackTrace();
						throw new JobExecutionException(e.toString());
					}
					catch (FileNotFoundException e)
					{
						deleteFile(fileName, csvFileCreated);
						e.printStackTrace();
						throw new JobExecutionException(e.toString());
					}
					catch (SecurityException e)
					{
						deleteFile(fileName, csvFileCreated);
						e.printStackTrace();
						throw new JobExecutionException(e.toString());
					}
				}
			}
			
		}
		catch (Exception e) 
		{
			deleteFile(fileName, csvFileCreated);
			
			if(job != null && hp != null)
			{
				try
				{
					job.setLastRunDateTime(now);
					hp.saveOrUpdate(new Object[] {job});
				}
				catch(Exception e1)
				{
					
				}
			}
			
			throw new JobExecutionException(e.toString());
		}
	}

	private void deleteFile(String fileName, boolean csvFileCreated)
	{
		if(csvFileCreated == true)
		{
			try
			{
				File f = new File(fileName);
				if(f.exists() && f.canWrite())
					f.delete();
			}
			catch(Exception ee)
			{
				
			}
		}
	}

	@SuppressWarnings("unchecked")
	private byte[] processExportFile(ArrayList ids, byte[] doc, String fileNameErr) throws IOException
	{
		StringBuffer sb = new StringBuffer(doc.length);
		HashMap mapId = new HashMap();
		String id = "";
		ids.clear();
		
		boolean firstField = true;
		for (int i = 0; i < doc.length; i++)
		{
			if(firstField == true)
			{
				if(doc[i] == ',')
				{
					firstField = false;
					
					if(extractIdFromField(id, mapId, fileNameErr) == false)
						return null;
					
					id = "";
				}
				else if(doc[i] == '\r' || doc[i] == '\n')
				{
					sb.append((char)doc[i]);
				}
				else
				{
					id += (char)doc[i];
				}
			}
			else if(doc[i] == '\r' || doc[i] == '\n')
			{
				firstField = true;
				sb.append((char)doc[i]);
			}
			else
			{
				sb.append((char)doc[i]);
			}
		}
		
		for (Iterator iter = mapId.keySet().iterator(); iter.hasNext();)
		{
			ids.add((Integer) iter.next());
		}
		
		return sb.toString().getBytes();
	}

	@SuppressWarnings("unchecked")
	private boolean extractIdFromField(String id, HashMap mapId, String fileNameErr) throws IOException
	{
		String tmp = "";
		if(id.startsWith("\"_PSLR_") == false)
		{
			writeErrFile(fileNameErr, "I could not extract ServiceActivity id, are the report and/or template up to date?");
			return false;
		}
		
		if(id.endsWith("\""))
		{
			if(7 <= id.length() - 1)
				tmp = id.substring(7, id.length() - 1);
		}
		else
			tmp = id.substring(7);
		
		try
		{
			mapId.put(Integer.valueOf(tmp),"");
		}
		catch(Exception e)
		{
			writeErrFile(fileNameErr, "I could not extract ServiceActivity id (conversion to int from string error), the report and/or template are up to date?\r\nTried to convert '" + tmp + "' to int.");
			return false;
		}
		
		return true;
	}

	private void writeErrFile(String fileNameErr, String msg) throws IOException
	{
		FileOutputStream fos = new FileOutputStream(fileNameErr);
		fos.write(msg.getBytes());
		fos.flush();
		fos.close();
	}

	private String getOutputFolder() throws JobExecutionException
	{
		if ( System.getProperty("catalina.home") == null)
		{
			throw new JobExecutionException("The JVM parameter 'catalina.home' was not found !");				
		}
		
		String folderName = System.getProperty("catalina.home") + "/TransactionExport/";				
		File folder = new File(folderName);
		
		if(folder.exists() == false)
		{
			boolean bSuccess = folder.mkdir();
			
			if(bSuccess == false)
				throw new JobExecutionException("I could not create the folder '" + folder.getAbsolutePath() + "' !");
		}
		
		return folderName;
	}
	
}
