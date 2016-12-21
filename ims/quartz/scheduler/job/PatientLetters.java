/*
 * Created on 03-Oct-2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package ims.quartz.scheduler.job;

import ims.configuration.gen.ConfigFlag;
import ims.core.admin.domain.objects.PrintAgent;
import ims.core.admin.domain.objects.PrintAgentDocuments;
import ims.quartz.scheduler.hibernate.HibernateProxy;

import java.util.ArrayList;
import java.util.Date;

import org.quartz.old.Job;
import org.quartz.old.JobExecutionContext;
import org.quartz.old.JobExecutionException;

import com.ims.query.builder.client.QueryBuilderClient;
import com.ims.query.builder.client.SeedValue;
import com.ims.query.builder.client.exceptions.QueryBuilderClientException;

public class PatientLetters implements Job
{
	private static final int nMaxBatchSize = 3;

	@SuppressWarnings("unchecked")
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
		
		Integer id = (Integer)context.getJobDetail().getJobDataMap().get("PrintAgentID");

		System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' got executed !!!");
		
		HibernateProxy hp = new HibernateProxy();

		ArrayList ids = hp.find("select distinct c1_1.id \r\n" + 
				"from CaseReferral as c1_1 join c1_1.patient as p1_1 left join p1_1.name.title as l1_1 left join c1_1.referringHospital as l2_1 left join c1_1.procedures as c2_1 left join c2_1.procedure as p3_1 left join c1_1.treatingHospital as l3_1\r\n" + 
				"where c1_1.systemInformation.creationUser <> \'DATA_TAKEON\' and \r\n" + 
				" c1_1.referringHospital is not null  and c1_1.treatingHospital is not null and \r\n" + 
				"(c1_1.id not in (select elements(pad.printedRecords)\r\n" +
				"from PrintAgentDocuments as pad\r\n" + 
				"where pad.printAgent.id = :ID))  and c1_1.creatingOutlier is null  and c1_1.creatingDischargeSummary is null", new String[] {"ID"}, new Object[] {id});
		
		ArrayList pat = hp.find("select distinct c1_1.patient.id \r\n" + 
				"from CaseReferral as c1_1 join c1_1.patient as p1_1 left join p1_1.name.title as l1_1 left join c1_1.referringHospital as l2_1 left join c1_1.procedures as c2_1 left join c2_1.procedure as p3_1 left join c1_1.treatingHospital as l3_1\r\n" + 
				"where c1_1.systemInformation.creationUser <> \'DATA_TAKEON\' and \r\n" + 
				" c1_1.referringHospital is not null  and c1_1.treatingHospital is not null and \r\n" + 
				"(c1_1.id not in (select elements(pad.printedRecords)\r\n" + 
				"from PrintAgentDocuments as pad\r\n" + 
				"where pad.printAgent.id = :ID))  and c1_1.creatingOutlier is null  and c1_1.creatingDischargeSummary is null", new String[] {"ID"}, new Object[] {id});

		String query = "select p1_1.id, p1_1.queryServerUrl, p1_1.reportServerUrl, t1_1.template.id, t1_1.printerName, t2_1.report.id, t2_1.templateXml, r1_1.reportXml\r\n" + 
		"from PrintAgent as p1_1 left join p1_1.templatePrinters as t1_1 left join t1_1.template as t2_1 left join t2_1.report as r1_1\r\n" + 
		"where \r\n" + 
		"(p1_1.id = :ID) ";
		ArrayList al = hp.find(query, new String[] {"ID"}, new Object[] {id});
		
		for(int i = 0; i < al.size(); i++)
		{
			Object[] obj = (Object[])al.get(i);

			if(obj[7] != null && obj[6] != null && obj[2] != null)
			{
				QueryBuilderClient qb = new QueryBuilderClient(queryServerUrl, null);
				qb.addSeed(new SeedValue("ID", id, Integer.class));
				String comment = ids.size() == 0 ? "No data to print !" : "";
				qb.addSeed(new SeedValue("COMMENT", comment , String.class));
				
				System.out.println(pat.size() + " letters have been printed !");
				
				try
				{
					byte[] doc = qb.buildReport((String)obj[7], (String)obj[6], reportServerUrl, QueryBuilderClient.FP3 , "", 1);
					boolean bRet = qb.printReport(doc, (String)obj[2], obj[4] == null ? "." : (String)obj[4], 1);
					
					PrintAgent job = (PrintAgent)hp.getDomainObject(PrintAgent.class, id);
					if(job == null)
					{
						System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not retrieve the job from the database !!!");
						return;
					}
					
					Date now = new Date();
					job.setLastRunDateTime(now);
					if(bRet)
						job.setLastSuccessfulRunDateTime(now);

					PrintAgentDocuments document = new PrintAgentDocuments();
					document.setDocument(new String(doc));
					document.setPrintAgent(job);
					document.getSystemInformation().setCreationDateTime(now);
					document.setPrintedLetters(new Integer(pat.size()));
					
					if(ids.size() > 0)
					{
						for(int q = 0; q < ids.size(); q++)
							document.getPrintedRecords().add(ids.get(q));
					}
					
					Object[] pojo = new Object[2];
					
					pojo[0] = job;
					pojo[1] = document;
					
					bRet = hp.saveOrUpdate(pojo);
					
					if(bRet)
						System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' executed succesfully !!!");
					else
						System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not update the job status in the database !!!");
				}
				catch (QueryBuilderClientException e)
				{
					e.printStackTrace();
				}
				
				
				//System.out.println(obj[1]);
				//System.out.println(obj[2]);
				//System.out.println(obj[4]);
			}
		}
	}
}
