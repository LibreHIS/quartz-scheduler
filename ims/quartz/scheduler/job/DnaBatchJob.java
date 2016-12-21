package ims.quartz.scheduler.job;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import ims.careuk.domain.objects.CatsReferral;
import ims.careuk.domain.objects.OrderInvAppt;
import ims.careuk.vo.lookups.AdditionalInvestigationAppointmentsStatus;
import ims.choose_book.domain.objects.ActionRequest;
import ims.chooseandbook.vo.lookups.ActionRequestType;
import ims.configuration.gen.ConfigFlag;
import ims.core.admin.domain.objects.PrintAgent;
import ims.core.admin.domain.objects.PrintAgentDocuments;
import ims.core.documents.domain.objects.PatientDocument;
import ims.core.documents.domain.objects.ServerDocument;
import ims.core.patient.domain.objects.Patient;
import ims.core.vo.lookups.DocumentCategory;
import ims.core.vo.lookups.DocumentCreationType;
import ims.core.vo.lookups.FileType;
import ims.core.vo.lookups.PreActiveActiveInactiveStatus;
import ims.core.vo.lookups.TaxonomyType;
import ims.domain.exceptions.StaleObjectException;
import ims.domain.impl.DomainImplFlyweightFactory;
import ims.domain.lookups.LookupInstance;
import ims.domain.lookups.LookupMapping;
import ims.framework.enumerations.SystemLogLevel;
import ims.framework.enumerations.SystemLogType;
import ims.framework.interfaces.ISystemLogWriter;
import ims.framework.utils.DateTime;
import ims.ocrr.orderingresults.domain.objects.OrderInvestigation;
import ims.ocrr.orderingresults.domain.objects.OrderedInvestigationStatus;
import ims.ocrr.vo.lookups.OrderInvStatus;
import ims.ocrr.vo.lookups.OrderMessageStatus;
import ims.quartz.scheduler.hibernate.HibernateProxy;
import ims.quartz.scheduler.hibernate.HibernateUtil3;
import ims.scheduling.domain.objects.Appointment_Status;
import ims.scheduling.domain.objects.Booking_Appointment;
import ims.scheduling.domain.objects.ExternalSystemEvent;
import ims.scheduling.helper.Uuid;
import ims.scheduling.vo.lookups.ExternalSystemEventTypes;
import ims.scheduling.vo.lookups.Status_Reason;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.ByteArrayPartSource;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.quartz.old.Job;
import org.quartz.old.JobExecutionContext;
import org.quartz.old.JobExecutionException;

import com.ims.query.builder.client.QueryBuilderClient;
import com.ims.query.builder.client.SeedValue;
import com.ims.query.builder.client.exceptions.QueryBuilderClientException;

public class DnaBatchJob implements Job 
{
	private static final int TIMEOUT = 1000 * 60 * 15;
	private static final int MAX_BUFFER_LIMIT_NO_WARNING = 1024*1024;
	
	public void execute(JobExecutionContext context) throws JobExecutionException 
	{
		String queryServerUrl = ConfigFlag.GEN.QUERY_SERVER_URL.getValue();
		String reportServerUrl = ConfigFlag.GEN.REPORT_SERVER_URL.getValue();
		
		ISystemLogWriter log = getLog();
		
		if(queryServerUrl == null || queryServerUrl.length() == 0 || queryServerUrl.equals(ConfigFlag.GEN.QUERY_SERVER_URL.getDefaultValue()))
		{
			System.out.println("The config flag QUERY_SERVER_URL was not set !");
			
			if(log != null)
				log.createSystemLogEntry(SystemLogType.QUARTZ_JOB, SystemLogLevel.ERROR, (new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "': The config flag QUERY_SERVER_URL was not set!");
			
			throw new JobExecutionException("The config flag QUERY_SERVER_URL was not set!");
		}
		
		if(reportServerUrl == null || reportServerUrl.length() == 0 || reportServerUrl.equals(ConfigFlag.GEN.REPORT_SERVER_URL.getDefaultValue()))
		{
			System.out.println("The config flag REPORT_SERVER_URL was not set !");
			
			if(log != null)
				log.createSystemLogEntry(SystemLogType.QUARTZ_JOB, SystemLogLevel.ERROR, (new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "': The config flag REPORT_SERVER_URL was not set!");
			
			throw new JobExecutionException("The config flag REPORT_SERVER_URL was not set !");
		}
		
		System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' got executed !!!");
		
		if(log != null)
			log.createSystemLogEntry(SystemLogType.QUARTZ_JOB, SystemLogLevel.INFORMATION, (new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' got executed !!!");
		
		HibernateProxy hp = new HibernateProxy();
		
		String query = "select p1_1.id, p1_1.queryServerUrl, p1_1.reportServerUrl, p1_1.dNAApptsInLastXhrs, t1_1.template.id, t1_1.printerName, t2_1.report.id, t2_1.templateXml, r1_1.reportXml from PrintAgent as p1_1 left join p1_1.templatePrinters as t1_1 left join t1_1.template as t2_1 left join t2_1.report as r1_1 where (p1_1.id = :ID)";
		Integer idJob = (Integer)context.getJobDetail().getJobDataMap().get("PrintAgentID");
		List<Object> al = hp.find(query, new String[] {"ID"}, new Object[] {idJob});
		
		PrintAgent job = (PrintAgent)hp.getDomainObject(PrintAgent.class, idJob);
		if(job == null)
		{
			System.out.println((new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not retrieve the job from the database !!!");
			
			if(log != null)
				log.createSystemLogEntry(SystemLogType.QUARTZ_JOB, SystemLogLevel.ERROR, (new Date()).toString() + " >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not retrieve the job from the database!!!");
			
			throw new JobExecutionException(" >> The job '" + context.getJobDetail().getDescription() + "' failed to run, could not retrieve the job from the database !!!");
		}
		
		for(int i = 0; i < al.size(); i++)
		{
			Object[] obj = (Object[])al.get(i);
			
			String repXml = (String) obj[8];
			String templXml = (String) obj[7];
			Integer apptsInLastHours = (Integer) obj[3];
			String printerName = (String) obj[5];
			
			if(templXml != null && repXml != null && apptsInLastHours != null)
			{
				List<Object> appointments = getAppointments(apptsInLastHours, hp);
				List<Object> listCats = getCatsReferralFromAppointments(appointments, hp);
				
				if(printDnaLetters(queryServerUrl, reportServerUrl, hp, repXml, templXml, printerName, listCats, job, log))
					saveJob(job, hp, appointments != null ? appointments.size() : 0, log);
				else
					saveJob(job, hp, null, log);
			}
		}
	}

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

	private void saveJob(PrintAgent job, HibernateProxy hp, Integer numberAppointments, ISystemLogWriter log) 
	{
		if(job == null)
			return;
		
		Date now = new Date();
		job.setLastRunDateTime(now);
		if(numberAppointments != null)
		{
			job.setLastSuccessfulRunDateTime(now);
			job.setNoOfRecordsUpdated(numberAppointments);
			
			if(log != null)
				log.createSystemLogEntry(SystemLogType.QUARTZ_JOB, SystemLogLevel.INFORMATION, (new Date()).toString() + " >> The job '" + job.getDescription() + "' has successfully completed and updated " + numberAppointments + " appointments with DNA status!");
		}
		
		Object[] pojo = new Object[1];
		pojo[0] = job;
		hp.saveOrUpdate(pojo);
	}

	private Boolean printDnaLetters(String queryServerUrl, String reportServerUrl,	HibernateProxy hp, String repXml, String templXml,	String printerName, List<Object> listCats, PrintAgent job, ISystemLogWriter log) throws JobExecutionException 
	{
		if(listCats == null)
			return true;
		
		QueryBuilderClient qb = new QueryBuilderClient(queryServerUrl, null);
		Object lastCatsReferralId = null;
		Object lastPID = null;
		
		List<Object> appointments = new ArrayList<Object>();
		
		for (Object object : listCats)
		{
			Object[] objCats = (Object[]) object;
			
			if(lastCatsReferralId != null && !lastCatsReferralId.equals(objCats[0]))
			{
				try
				{
					qb.addSeed(new SeedValue("PID", lastPID , Integer.class));
					byte[] doc = qb.buildReport(repXml, templXml, reportServerUrl, QueryBuilderClient.FP3 , printerName, 1);
					byte[] pdfDoc = qb.convertReport(reportServerUrl, doc, QueryBuilderClient.PDF, "", 1);
					
					saveDnaAppointments(appointments, lastCatsReferralId, hp);
					saveDocuments(lastCatsReferralId, lastPID, doc, pdfDoc, hp, job, log);
				}
				catch (QueryBuilderClientException e)
				{
					e.printStackTrace();
					
					if(log != null)
						log.createSystemLogEntry(SystemLogType.QUARTZ_JOB, SystemLogLevel.ERROR, (new Date()).toString() + " >> The job '" + job.getDescription() + "': Query Builder Client Exception!");
					
					return false;
				}
				
				qb.getSeeds().clear();
				appointments.clear();
			}
			
			qb.addSeed(new SeedValue("APPT_ID", objCats[2] , Integer.class));
			appointments.add(objCats[2]);
			
			lastPID = objCats[1];
			lastCatsReferralId = objCats[0];
		}
		
		if(qb.getSeeds().size() > 0)
		{
			try
			{
				qb.addSeed(new SeedValue("PID", lastPID , Integer.class));
				byte[] doc = qb.buildReport(repXml, templXml, reportServerUrl, QueryBuilderClient.FP3 , printerName, 1);
				byte[] pdfDoc = qb.convertReport(reportServerUrl, doc, QueryBuilderClient.PDF, "", 1);
				
				saveDnaAppointments(appointments, lastCatsReferralId, hp);
				saveDocuments(lastCatsReferralId, lastPID, doc, pdfDoc, hp, job, log);
			}
			catch (QueryBuilderClientException e)
			{
				e.printStackTrace();
				
				if(log != null)
					log.createSystemLogEntry(SystemLogType.QUARTZ_JOB, SystemLogLevel.ERROR, (new Date()).toString() + " >> The job '" + job.getDescription() + "': Query Builder Client Exception!");
				
				return false;
			}
			
			qb.getSeeds().clear();
		}
		
		return true;
	}

	private List<Object> getCatsReferralFromAppointments(List<Object> appointments,	HibernateProxy hp) 
	{
		if(appointments == null || appointments.size() == 0)
			return null;
		
		StringBuilder sb = new StringBuilder();
		sb.append("select c1_1.id, c1_1.patient.id, b1_1.id from CatsReferral as c1_1 join c1_1.appointments as b1_1 where ");
		sb.append(" b1_1.id in (");
		
		for (int i=0; i<appointments.size(); i++)
		{
			Booking_Appointment appt = (Booking_Appointment) appointments.get(i);
			
			if(i > 0)
				sb.append(",");
			
			sb.append(appt.getId().toString());
		}
		
		sb.append(")");
		sb.append(" order by c1_1.id asc, c1_1.patient asc, b1_1.id asc ");
		
		return hp.find(sb.toString());
	}

	private List<Object> getAppointments(Integer apptsInLastHours, HibernateProxy hp) 
	{
		GregorianCalendar apptDate = new GregorianCalendar();
		apptDate.add(Calendar.HOUR, -apptsInLastHours);
		
		String query = "from Booking_Appointment as b where b.appointmentDate between :date1 and :date2 and b.apptStatus.id = :status1";
		List<Object> appointments = hp.find(query, new String[] {"date1", "date2", "status1"}, new Object[] {apptDate.getTime(), new Date(), Status_Reason.BOOKED.getID()});
		
		if(appointments == null || appointments.size() == 0)
			return null;
		
		List<Object> dnaAppointments = new ArrayList<Object>();
		
		for (int i=0; i<appointments.size(); i++)
		{
			Booking_Appointment appt = (Booking_Appointment) appointments.get(i);
			
			if(appt.getApptStartTime() == null)
			{
				dnaAppointments.add(appt);
				continue;
			}
			
			DateTime apptStartTime=new DateTime();
			apptStartTime.setDateTime(new ims.framework.utils.Date(appt.getAppointmentDate()), new ims.framework.utils.Time(appt.getApptStartTime()));
			
			if (apptStartTime.compareTo(new DateTime(apptDate.getTime())) >= 0 && apptStartTime.compareTo(new DateTime()) <= 0)
			{
				dnaAppointments.add(appt);
			}
		}
		
		return dnaAppointments;
	}

	private void saveDocuments(Object lastCatsReferralId, Object lastPID, byte[] doc, byte[] pdfDoc, HibernateProxy hp, PrintAgent job, ISystemLogWriter log) 
	{
		if(lastCatsReferralId == null || lastPID == null)
			return;
		
		String urlPdfUploadServer = ConfigFlag.GEN.PDF_UPLOAD_URL.getValue();
		if (urlPdfUploadServer == null || urlPdfUploadServer.length() == 0)
		{
			System.out.println("Cannot upload. The config flag PDF_UPLOAD_URL was not set !");
			
			if(log != null)
				log.createSystemLogEntry(SystemLogType.QUARTZ_JOB, SystemLogLevel.ERROR, (new Date()).toString() + " >> The job '" + job.getDescription() + "': Cannot upload. The config flag PDF_UPLOAD_URL was not set!");
		}

		String fileName = generateName() + ".pdf";
		
		uploadFile(pdfDoc, fileName, urlPdfUploadServer);
		
		Patient patient = (Patient) hp.getDomainObject(Patient.class, (Integer) lastPID);
		
		PatientDocument document = populatePatientDocument(populateServerDocumentVo(fileName, hp), patient, hp);
		PrintAgentDocuments printAgentDoc = createPrintAgentDoc(patient, doc, job);
		
		savePatientDocuments(document, printAgentDoc, lastCatsReferralId, hp);
	}

	private PrintAgentDocuments createPrintAgentDoc(Patient patient, byte[] doc, PrintAgent job) 
	{
		PrintAgentDocuments document = new PrintAgentDocuments();
		document.setDocument(new String(doc));
		document.setPrintAgent(job);
		document.getSystemInformation().setCreationDateTime(new Date());
		document.setPrintedLetters(1);
		document.setDescription(patient.getName().toString());
		
		return document;
	}

	private void uploadFile(byte[] file, String fileName, String localFolder) 
	{
		if (file == null || file.length == 0 || fileName == null || localFolder == null) 
		{
			return;
		}
		
		if(localFolder == null || localFolder.length() == 0)
		{
			return;
		}
		
		HttpClient conn = null;	  				
		
		PostMethod filePost = new PostMethod(localFolder);
		conn = new HttpClient(new MultiThreadedHttpConnectionManager());
		conn.getHttpConnectionManager().getParams().setConnectionTimeout(TIMEOUT);		
		conn.getParams().setBooleanParameter(HttpMethodParams.USE_EXPECT_CONTINUE, true);
		conn.getParams().setIntParameter(HttpMethodParams.BUFFER_WARN_TRIGGER_LIMIT, MAX_BUFFER_LIMIT_NO_WARNING);
		  	  	  
		Part[] data = 
		{
		    new StringPart("name", localFolder), 
		    new StringPart("filename", fileName),	           
		    new FilePart(fileName, new ByteArrayPartSource(fileName, file))	           
		};
		  		
		filePost.setRequestEntity(new MultipartRequestEntity(data, filePost.getParams()));
	  		
		try
		{
			conn.executeMethod(filePost);
		} 
		catch (HttpException e)
		{	
			e.printStackTrace();
			return;
		} 
		catch (IOException e)
		{	
			e.printStackTrace();
			return;
		}
		finally
		{
			filePost.releaseConnection();
		}
	}
	
	private void savePatientDocuments(PatientDocument document, PrintAgentDocuments printAgentDoc, Object idCatsReferral, HibernateProxy hp)
	{
		if(document == null || idCatsReferral == null)
			return;
		
		Object[] pojo = new Object[3];
		
		CatsReferral catsReferral = getCatsReferralObject((Integer) idCatsReferral, document);
		
		pojo[0] = document;
		pojo[1] = printAgentDoc;
		pojo[2] = catsReferral;
			
		hp.saveOrUpdate(pojo);
	}

	private CatsReferral getCatsReferralObject(Integer idCatsReferral, PatientDocument document) 
	{
		if(document == null || idCatsReferral == null)
			return null;
		
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
			CatsReferral cats = (CatsReferral) session.get(CatsReferral.class, idCatsReferral);
			cats.getReferralDocuments().add(document);	
			cats.setHasDocuments(Boolean.TRUE);
			
			tx.commit();
			return cats;
		}
		catch(HibernateException e)
		{
			e.printStackTrace();
			HibernateUtil3.handleHibernateException(tx, e);
			return null;
		}
		finally
		{
			try
			{
				HibernateUtil3.closeSession();
			}
			catch (HibernateException e2)
			{
				e2.printStackTrace();
			}
		}
	}

	private PatientDocument populatePatientDocument(ServerDocument serverDocumentVo, Patient patient, HibernateProxy hp) 
	{
		PatientDocument document = new PatientDocument();
		
		document.setPatient(patient);
		document.setEpisodeofCare(null);
		document.setCareContext(null);
		document.setClinicalContact(null);
		document.setReferral(null);
				
		document.setName("Letter to patient DNA");
		document.setServerDocument(serverDocumentVo);
		document.setCreationType((LookupInstance) hp.getDomainObject(LookupInstance.class, DocumentCreationType.GENERATED.getID()));
		document.setCategory((LookupInstance) hp.getDomainObject(LookupInstance.class, DocumentCategory.LETTER_TO_PATIENT_DNA.getID()));
		
		document.setRecordingUser(null);
		document.setRecordingDateTime(new Date());
		document.setStatus((LookupInstance) hp.getDomainObject(LookupInstance.class, PreActiveActiveInactiveStatus.ACTIVE.getID()));
		
		return document;
	}

	private ServerDocument populateServerDocumentVo(String fileName, HibernateProxy hp) 
	{
		ims.framework.utils.Date date = new ims.framework.utils.Date();
		int year = date.getYear();
		int month = date.getMonth();
		int day = date.getDay();			

		ServerDocument server = new ServerDocument();
		String filePath = year + "/" + month + "/" + day + "/" + fileName;			
		server.setFileName(filePath);
		server.setFileType((LookupInstance) hp.getDomainObject(LookupInstance.class, FileType.PDF.getID()));
		
		return server;	
	}

	private String generateName() 
	{
		String str = "";

		try
		{
			//Get Random Segment
			SecureRandom prng = SecureRandom.getInstance("SHA1PRNG");
			str += Integer.toHexString(prng.nextInt());
			while (str.length() < 8)
			{
				str = '0' + str;
			}

			//Get CurrentTimeMillis() segment
			str += Long.toHexString(System.currentTimeMillis());
			while (str.length() < 12)
			{
				str = '0' + str;
			}

			//Get Random Segment
			SecureRandom secondPrng = SecureRandom.getInstance("SHA1PRNG");
			str += Integer.toHexString(secondPrng.nextInt());
			while (str.length() < 8)
			{
				str = '0' + str;
			}

			//Get IdentityHash() segment
			str += Long.toHexString(System.identityHashCode((Object) this));
			while (str.length() < 8)
			{
				str = '0' + str;
			}
			//Get Third Random Segment
			byte bytes[] = new byte[16];
			SecureRandom thirdPrng = SecureRandom.getInstance("SHA1PRNG");
			thirdPrng.nextBytes(bytes);
			str += Integer.toHexString(thirdPrng.nextInt());
			while (str.length() < 8)
			{
				str = '0' + str;
			}
		}
		catch (java.security.NoSuchAlgorithmException ex)
		{
			ex.getMessage();
		}

		return str;
	}

	private void saveDnaAppointments(List<Object> appointments, Object lastCatsReferralId, HibernateProxy hp) throws JobExecutionException 
	{
		if(appointments == null)
			return;
		
		for (int i=0; i<appointments.size(); i++)
		{
			try 
			{
				saveBookingAppt(appointments.get(i), hp);
			} 
			catch (StaleObjectException e) 
			{
				e.printStackTrace();
				throw new JobExecutionException(e.toString());
			}
		}
		
		if(lastCatsReferralId != null)
		{
			saveCatsReferral(lastCatsReferralId, hp);
		}
	}

	private void saveCatsReferral(Object lastCatsReferralId, HibernateProxy hp) 
	{
		if(lastCatsReferralId == null)
			return;
		
		CatsReferral catsReferral = (CatsReferral) hp.getDomainObject(CatsReferral.class, (Integer) lastCatsReferralId);
		catsReferral.setHasRebookingSubsequentActivity(Boolean.TRUE);
		
		Object[] pojo = new Object[1];
		pojo[0] = catsReferral;
		
		hp.saveOrUpdate(pojo);
	}

	private void saveBookingAppt(Object apptId, HibernateProxy hp) throws StaleObjectException
	{
		if(apptId == null)
			return;
		
		MyArrayList <Object> pojoArray = new MyArrayList <Object>();
		
		Session session = HibernateUtil3.currentSession();;
		Transaction tx = session.beginTransaction();
		
		try
		{
			Booking_Appointment appt = (Booking_Appointment) session.get(Booking_Appointment.class, (Integer) apptId);
			appt.setApptStatus((LookupInstance) session.get(LookupInstance.class, Status_Reason.DNA.getID()));
			
			Appointment_Status historyStatus = new Appointment_Status();
			historyStatus.setStatus((LookupInstance) session.get(LookupInstance.class, Status_Reason.DNA.getID()));
			historyStatus.setApptDate(appt.getAppointmentDate());
			historyStatus.setApptTime(appt.getApptStartTime());
			historyStatus.setStatusChangeDateTime((new DateTime()).getJavaDate());
			
			appt.getApptStatusHistory().add(historyStatus);
			
			if(appt.isIsCABBooking() == null || appt.isIsCABBooking() == false)
			{
				appt.setRequiresRebook(true);
				if(appt.getSessionSlot() != null)
				{
					appt.getSessionSlot().setStatus((LookupInstance) session.get(LookupInstance.class, Status_Reason.DNA.getID()));
					//appt.getSessionSlot().setAppointment(null);	WDEV-12090
					//appt.setSessionSlot(null); 	WDEV-12090
				}
				else if(appt.getTheatreSlot() != null)
				{
					appt.getTheatreSlot().setStatus((LookupInstance) session.get(LookupInstance.class, Status_Reason.DNA.getID()));
					appt.getTheatreSlot().setAppointment(null);
					appt.setTheatreSlot(null);
				}
			}
			
			pojoArray.add(appt);
			
			Query q = session.createQuery("from OrderInvAppt ordInvAppt where ordInvAppt.appointment.id = " + appt.getId());
			Iterator it = q.list().iterator();
			
			while(it.hasNext())
			{
				OrderInvAppt orderInvAppt = (OrderInvAppt) it.next();
				
				pojoArray.add(saveOrderPatientDNAEvent(orderInvAppt.getAppointment(), orderInvAppt.getOrderInvestigation(), session));
				pojoArray.add(saveCatsReferralwithAddtionalInvApptStatus(orderInvAppt, session));	
				pojoArray.add(saveOrderInvApp(orderInvAppt, session));
			}
			
			if(ConfigFlag.GEN.ICAB_ENABLED.getValue())
			{
				if(appt.isIsCABBooking() != null && appt.isIsCABBooking())
				{
					pojoArray.add(sendRequestandUpdateReferences(session, appt));
				}
			}
			
			tx.commit();
		}
		catch(HibernateException e)
		{
			e.printStackTrace();
			HibernateUtil3.handleHibernateException(tx, e);
		}
		finally
		{
			try
			{
				HibernateUtil3.closeSession();
			}
			catch (HibernateException e2)
			{
				e2.printStackTrace();
			}
		}
		
		Object[] pojo = new Object[pojoArray.size()];
		pojoArray.toArray(pojo);
		
		hp.saveOrUpdate(pojo);
	}

	private Object sendRequestandUpdateReferences(Session session, Booking_Appointment appt) throws StaleObjectException
	{
		if(ims.configuration.gen.ConfigFlag.DOM.SCHEDULING_SLOTS_CREATION.getValue().equals("Choose and Book"))
		{
			if (appt != null)
			{
				LookupInstance notify_dna = (LookupInstance) session.get(LookupInstance.class, ActionRequestType.NOTIFY_DNA.getID());
				
				return placeOutgoingRequest(session, notify_dna, appt.buildCabMessage(notify_dna), "Appt DNA Requested from DNA Batch Update Job");
			}
		}
		
		return null;
	}

	private ActionRequest placeOutgoingRequest(Session session, LookupInstance requestType, String msgDetails, String statComments) 
	{
		if (!(ConfigFlag.GEN.ICAB_ENABLED.getValue()))
			return null;
		
		if (msgDetails == null)
			return null; //throw new DomainRuntimeException("CAB Action Request cannot be placed without message details");
		
		String defaultValue=null;
		
		if (requestType.getId() == ActionRequestType.NOTIFY_DNA.getId())
			defaultValue = "9";  // DNA Default - not specified

		// Get the External Mapping for reason for DNA and Cancelled
		if (defaultValue != null)
		{
			int reasonIdx = msgDetails.indexOf("REASON:");
			int endIdx = reasonIdx + msgDetails.substring(reasonIdx).indexOf(";");
			String cancelReason = msgDetails.substring(reasonIdx + 7, endIdx);
			StringBuffer newMsgDetails = new StringBuffer(msgDetails.substring(0, reasonIdx));
			String externalReason = getExternalMapping(session, cancelReason, defaultValue);

			newMsgDetails.append("REASON:" + externalReason + ";");
			msgDetails = newMsgDetails.toString();
		}
		
		ActionRequest req= new ActionRequest();
		req.setActive(Boolean.TRUE);
		req.setRequestDate(new java.util.Date());
		req.setRequestType(requestType);
		req.setMsgDetails(msgDetails);
		req.setConvId(Uuid.generateUUID());
		req.setCpaId(null);
		req.setStatComment(statComments);

		return req;
	}
	
	private String getExternalMapping(Session session, String cancelReason, String defaultValue) 
	{
		if (cancelReason == null || cancelReason.equals(""))
			return defaultValue;
		
		Integer lookupInstanceId = null;
		
		try
		{
			lookupInstanceId = Integer.parseInt(cancelReason);
		
		}
		catch(NumberFormatException e)
		{
			return defaultValue;
		}
		
		LookupInstance inst = (LookupInstance) session.get(LookupInstance.class, lookupInstanceId);
		
		if (inst != null)
		{
			LookupMapping map = inst.getMapping(TaxonomyType.ICAB.getText());
			if (map != null)
				return (map.getExtCode());
		}
		
		return defaultValue;
	}

	private OrderInvAppt saveOrderInvApp(OrderInvAppt orderInvAppt, Session session) 
	{
		if(orderInvAppt == null)
			return null;
		
		OrderedInvestigationStatus ordInvStatus = new OrderedInvestigationStatus();
		ordInvStatus.setChangeDateTime((new DateTime()).getJavaDate());
		ordInvStatus.setProcessedDateTime(new java.util.Date());
		ordInvStatus.setOrdInvStatus((LookupInstance) session.get(LookupInstance.class, OrderInvStatus.HOLD_REQUESTED.getID()));
		ordInvStatus.setStatusReason("Patient DNA'd associated Appt");
		
		orderInvAppt.getOrderInvestigation().setOrdInvCurrentStatus(ordInvStatus);
		orderInvAppt.getOrderInvestigation().getOrdInvStatusHistory().add(ordInvStatus);
		
		return orderInvAppt;
	}

	private CatsReferral saveCatsReferralwithAddtionalInvApptStatus(OrderInvAppt doOrderInvAppt, Session session) 
	{
		if(doOrderInvAppt == null)
			return null;
		
		List<Object> lstCatsRef = session.createQuery("from CatsReferral catsRef join fetch catsRef.orderInvAppts ordInvAppt where ordInvAppt.id = '" + doOrderInvAppt.getId() + "'").list();
		
		if(lstCatsRef != null && lstCatsRef.size() == 1)
		{
			CatsReferral doCatsRef = (CatsReferral) lstCatsRef.get(0);
			doCatsRef.setAdditionalInvApptsStatus((LookupInstance) session.get(LookupInstance.class, AdditionalInvestigationAppointmentsStatus.DNA.getID()));
			
			return doCatsRef;
		}
		
		return null;
	}

	private ExternalSystemEvent saveOrderPatientDNAEvent(Booking_Appointment booking_Appointment, OrderInvestigation orderInvestigation, Session session) 
	{
		ExternalSystemEvent event = new ExternalSystemEvent();
		
		if (orderInvestigation != null) 
		{
			if(orderInvestigation.getInvestigation() != null && orderInvestigation.getInvestigation().getInvestigationIndex() != null &&  orderInvestigation.getInvestigation().getInvestigationIndex().isNoInterface() != null && orderInvestigation.getInvestigation().getInvestigationIndex().isNoInterface())
				return null;
				
			event.setInvestigation(orderInvestigation);
			event.setProviderSystem(orderInvestigation.getInvestigation().getProviderService().getProviderSystem());
		}

		event.setAppointment(booking_Appointment);
		event.setWasProcessed(Boolean.FALSE);
		event.setMessageStatus((LookupInstance) session.get(LookupInstance.class, OrderMessageStatus.CREATED.getID()));
		event.setEventType((LookupInstance) session.get(LookupInstance.class, ExternalSystemEventTypes.PATIENTDNA.getID()));	
			
		return event;
	}
	
	class MyArrayList<T> extends ArrayList<T>
	{
		private static final long serialVersionUID = 1L;
	
		@Override
		public boolean add(T arg) 
		{
			if (arg == null)
				return false;
			
			return super.add(arg);
		}
	}
}