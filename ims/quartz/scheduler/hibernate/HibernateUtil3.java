/*
 * Created on 07-Oct-2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package ims.quartz.scheduler.hibernate;

import ims.domain.hibernate3.Registry;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

/**
 * @author vpurdila
 * 
 */
public class HibernateUtil3
{
	private static SessionFactory sessionFactory;

	public static final ThreadLocal session = new ThreadLocal();

	public static Session currentSession() throws HibernateException
	{
		Session s = (Session) session.get();
		if (s == null)
		{
			if (sessionFactory == null)
				sessionFactory = Registry.getInstance().getSessionFactory();

			s = sessionFactory.openSession();
			session.set(s);
		}
		return s;
	}

	public static void closeSession() throws HibernateException
	{
		Session s = (Session) session.get();
		session.set(null);
		if (s != null)
			s.close();
	}

	public static void handleHibernateException(Transaction tx,
			HibernateException e)
	{
		if (tx != null)
		{
			try
			{
				tx.rollback();
			}
			catch (HibernateException e1)
			{
				// Do nothing since we're already handling an exception.
			}
		}
		
		//throw e;
	}
}