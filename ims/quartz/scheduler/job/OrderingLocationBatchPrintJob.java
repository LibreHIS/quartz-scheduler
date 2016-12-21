package ims.quartz.scheduler.job;

import ims.configuration.gen.ConfigFlag;
import ims.core.admin.domain.objects.PrintAgent;
import ims.domain.impl.DomainImplFlyweightFactory;
import ims.framework.enumerations.SystemLogLevel;
import ims.framework.enumerations.SystemLogType;
import ims.framework.interfaces.ISystemLog;
import ims.framework.interfaces.ISystemLogWriter;
import ims.ocrr.orderingresults.domain.objects.OrderingLocationBatchPrint;
import ims.quartz.scheduler.hibernate.HibernateProxy;
import ims.quartz.scheduler.hibernate.HibernateUtil3;

import java.security.Security;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.quartz.old.JobExecutionContext;
import org.quartz.old.JobExecutionException;
import org.quartz.old.StatefulJob;

import com.ims.query.builder.client.QueryBuilderClient;
import com.ims.query.builder.client.SeedValue;
import com.ims.query.builder.client.exceptions.QueryBuilderClientException;

public class OrderingLocationBatchPrintJob implements StatefulJob
{
	private static int	RESULT_DETAILS = 75;
	
	private static String 	 SMTP_HOST_NAME;//WDEV-14081
	private static int 		 SMTP_PORT;//WDEV-14081
	private static String 	 SMTP_AUTH;//WDEV-14081
	private static String 	 emailFromAddress;	//WDEV-14081
	
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
		
		System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' got executed !!!");
		
		ISystemLogWriter log = getLog();//WDEV-14081
		
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
		
		Object[] resultDetails = getSystemReportAndTemplate(hp, RESULT_DETAILS);
		
		for(int i = 0; i < al.size(); i++)
		{
			Object[] obj = (Object[])al.get(i);
			
			String repXml = (String) obj[7];
			String templXml = (String) obj[6];
			String printerName = (String) obj[4];
			
			if(templXml != null && repXml != null)
			{
				QueryBuilderClient qb = new QueryBuilderClient(queryServerUrl, null);
								
				printReport(hp, qb, reportServerUrl, resultDetails, job, log);//WDEV-14081
				saveJob(job, hp);				
			}
		}
	}
	
	private void printReport(HibernateProxy hp, QueryBuilderClient qb, String reportServerUrl, Object[] resultDetailsCoverSheetReport, PrintAgent job, ISystemLogWriter log) throws JobExecutionException 
	{
		java.util.Date date = new java.util.Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		String timestamp = df.format(date);
		
		String fileName = "ResultOrderingLocationBatchPrint_" + job.getId() + "_" + timestamp + ".pdf";	
		String resultOrderingLocationBatchPrintReportXML = (String) resultDetailsCoverSheetReport[0];
		String resultOrderingLocationBatchPrintTemplateXML = (String) resultDetailsCoverSheetReport[1];

		//get a list of all records and print reports for each that have a printer specified - then delete the record
		String query = "select ordInv.id, loc.designatedPrinterForNewResults.name, olbp.id from OrderingLocationBatchPrint as olbp left join olbp.resultToBePrinted as ordInv left join olbp.ward as loc";
		
		StringBuffer sb = new StringBuffer();//WDEV-14081
		String printerName = "";
		List<Object> al = hp.find(query);
		
		for(int i=0;i<al.size();i++)
		{
			Object[] arrIdandWard = (Object[]) al.get(i);
			printerName = (String) arrIdandWard[1];
			
			//if the designated default printer has not been specified then do nothing with this record
			if(printerName != null && !printerName.equals(""))
			{
				qb.getSeeds().clear();	
				qb.addSeed(new SeedValue("OrderInvestigation_id", arrIdandWard[0] , Integer.class));
				
				try 
				{
					qb.buildReport(resultOrderingLocationBatchPrintReportXML, resultOrderingLocationBatchPrintTemplateXML, reportServerUrl, QueryBuilderClient.PDF , printerName, 1);
					//delete the record when it has been sent to the printer specified
					deleteOrderingLocationBatchPrintRecord(hp, (Integer)arrIdandWard[2]);
				} 
				catch (QueryBuilderClientException e) 
				{
					e.printStackTrace();
					//throw new JobExecutionException(e.toString());	WDEV-14081
					
					ISystemLog logEntry = log.createSystemLogEntry(SystemLogType.REPORTS, SystemLogLevel.ERROR, (new Date()).toString() + " >> The job '" + job.getDescription() + "' failed to run, when trying to pring Order Investigation with Id = " + arrIdandWard[0] + " on the printer " + printerName + " - " + e.getMessage());//WDEV-14081
					
					//WDEV-14081
					if(logEntry != null)
					{
						sb.append((sb.length() > 0 ? "\n\r" : "") + "An error occured when trying to build/print a report from within a Ordering Location Batch Job. Please check the system log entry with ID = " + logEntry.getSystemLogEventId());
					}
				}
			}
		}
		
		//WDEV-14081
		if(sb.length() > 0)
		{
			sendNotificationMail(sb.toString(), job, log);
		}
	}
	
	//WDEV-14081
	private void sendNotificationMail(String message, PrintAgent job, ISystemLogWriter log) 
	{
		if(message == null || message.length() == 0)
			return;
		
		String mailRecipient = ConfigFlag.DOM.BATCH_JOB_NOTIFICATION_EMAIL_ADDRESSES.getValue();
		
		if(mailRecipient == null || mailRecipient.length() == 0)
			return;
		
		String mailSubject = "Ordering Location batch printing error";
		
		try 
		{
			new SendMail().sendSSLMessage(mailRecipient, mailSubject, message, "");
		} 
		catch (MessagingException e) 
		{
			e.printStackTrace();
			
			if(log != null && job != null)
			{
				log.createSystemLogEntry(SystemLogType.REPORTS, SystemLogLevel.ERROR, (new Date()).toString() + " >> The job '" + job.getDescription() + "' could not send the e-mail with Ordering Location batch printing error to " + mailRecipient + ", " + e.getMessage());
			}
		}
	}

	private void deleteOrderingLocationBatchPrintRecord(HibernateProxy hp, Integer idOrderingLocationBatchPrint)
	{
		try
		{
			OrderingLocationBatchPrint recordtoBeDeteled = (OrderingLocationBatchPrint) hp.getDomainObject(OrderingLocationBatchPrint.class, idOrderingLocationBatchPrint);		
			hp.delete(recordtoBeDeteled);
		}
		catch (HibernateException ex)
		{
			//do nothing here - record possibly deleted by another job
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
	
	//WDEV-14081
	private ISystemLogWriter getLog() 
	{
		ISystemLogWriter log = null;
		
		try 
		{
			Class<?> cls = Class.forName("ims.admin.domain.impl.BatchPrintingImpl");
			log = (ISystemLogWriter)DomainImplFlyweightFactory.getInstance().create(cls, ims.domain.DomainSession.getSession());
		} 
		catch (InstantiationException e) 
		{
			e.printStackTrace();
		} 
		catch (IllegalAccessException e) 
		{
			e.printStackTrace();
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		
		return log;
	}
	
	//WDEV-14081
	private class SendMail 
	{
		private SendMail()
		{
			Security.addProvider(new com.sun.net.ssl.internal.ssl.Provider());
			
			SMTP_HOST_NAME 		= ConfigFlag.FW.SMTP_SERVER.getValue();
			SMTP_PORT 			= ConfigFlag.FW.SMTP_PORT.getValue();
			SMTP_AUTH 			= ConfigFlag.FW.SMTP_AUTH.getValue();
			emailFromAddress 	= ConfigFlag.FW.SMTP_SENDER.getValue();	
		}
				
		public void sendSSLMessage(String recipient, String subject, String message, String atach) throws MessagingException 
		{
			System.out.println("Send email to: " + recipient + " with subject :" + subject + " and message : " + message + " and attach file: " + atach);
			
			boolean debug = false;									
			Properties props = new Properties();
			props.put("mail.host", SMTP_HOST_NAME);
			props.put("mail.smtp.auth", "true");
			props.put("mail.debug", "false");
			props.put("mail.smtp.port", SMTP_PORT);
			props.put("mail.smtp.socketFactory.port", SMTP_PORT);
			props.put("mail.smtp.socketFactory.fallback", "false");

			javax.mail.Session session = javax.mail.Session.getDefaultInstance(props,
			new javax.mail.Authenticator() 
			{
				protected javax.mail.PasswordAuthentication getPasswordAuthentication() 
				{
					String[] auth = SMTP_AUTH.split(":");
					return new javax.mail.PasswordAuthentication(auth[0], auth[1]);
				}
			});

			session.setDebug(debug);

			javax.mail.Message msg = new MimeMessage(session);
			InternetAddress addressFrom = new InternetAddress(emailFromAddress);
			msg.setFrom(addressFrom);

			InternetAddress addressTo = new InternetAddress(recipient);
			msg.setRecipient(javax.mail.Message.RecipientType.TO, addressTo);

			//Setting the Subject and Content Type
			msg.setSubject(subject);
			msg.setContent(message, "text/plain");
			
			//create and fill the first message part
			MimeBodyPart firstMsgBodyPart = new MimeBodyPart();
			firstMsgBodyPart.setText(message);

			//create the Multipart and add its parts to it
			Multipart mp = new MimeMultipart();
			mp.addBodyPart(firstMsgBodyPart);
			//mp.addBodyPart(secondMsgBodyPart);

			//add the Multipart to the message
			msg.setContent(mp);

			//set the Date: header
			msg.setSentDate(new Date());

			Transport tr = session.getTransport("smtp");
			tr.connect(SMTP_HOST_NAME, SMTP_PORT, null, null);
			msg.saveChanges();
			tr.sendMessage(msg, msg.getAllRecipients());
			tr.close();
		}
	}
}
