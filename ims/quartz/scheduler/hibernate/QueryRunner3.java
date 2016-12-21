/*
 * Created on 07-Oct-2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package ims.quartz.scheduler.hibernate;

import java.util.ArrayList;
import java.util.Iterator;

import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;

public class QueryRunner3 implements QueryRunner
{
	@SuppressWarnings("unchecked")
	public ArrayList find(String query, String[] paramNames, Object[] paramValues)
	{
		Transaction tx = null;
		Iterator it = null;
		Session session = null;
		ArrayList result = new ArrayList();

		session = HibernateUtil3.currentSession();
		
		tx = session.beginTransaction();
		try
		{
			Query q = session.createQuery(query);
			
			if(paramNames != null)
			{
				for(int i = 0; i < paramNames.length; i++)
				{
					q.setParameter(paramNames[i], paramValues[i]);
				}
			}
	
			it = q.list().iterator();
			tx.commit();
		}
		catch(HibernateException e)
		{
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
			}
		}
		
		while ( it.hasNext() ) 
		{
			Object row = it.next();
			
			result.add(row);
		}
		
		return result;
	}

	@SuppressWarnings("unchecked")
	public ArrayList find(String query)
	{
		return find(query, null, null);
	}

	public boolean saveOrUpdate(Object[] pojo)
	{
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
			for(int i = 0; i < pojo.length; i++)
			{
				if(pojo[i] instanceof HqlBatchUpdate)
				{
					HqlBatchUpdate hbu = (HqlBatchUpdate)pojo[i];
					
					Query q = session.createQuery( hbu.getHqlUpdate() );
					if(hbu.getParamNames() != null && hbu.getParamValues() != null)
					{
						if(hbu.getParamNames().length != hbu.getParamValues().length)
							throw new RuntimeException("The number of param names is not equal to the number of param values !");
						
						for(int a = 0; a < hbu.getParamNames().length; a++)
						{
							q.setParameter(hbu.getParamNames()[a], hbu.getParamValues()[a]);
						}
					}
                    q.executeUpdate();					
				}
				else
					session.saveOrUpdate(pojo[i]);
			}
			
			tx.commit();
			return true;
		}
		catch(HibernateException e)
		{
			e.printStackTrace();
			HibernateUtil3.handleHibernateException(tx, e);
			return false;
		}
		finally
		{
			try
			{
				HibernateUtil3.closeSession();
			}
			catch (HibernateException e2)
			{
			}
		}
	}

	@SuppressWarnings("unchecked")
	public Object getDomainObject(Class clazz, int id)
	{
		return getDomainObject(clazz, new Integer(id));
	}

	@SuppressWarnings("unchecked")
	public Object getDomainObject(Class clazz, Integer id)
	{
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
			Object obj = session.get(clazz, id);
			
			tx.commit();
			return obj;
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
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public ArrayList findSql(String query, String[] paramNames, Object[] paramValues)
	{
		Transaction tx = null;
		Iterator it = null;
		Session session = null;
		ArrayList result = new ArrayList();

		session = HibernateUtil3.currentSession();
		
		tx = session.beginTransaction();
		try
		{
			Query q = session.createSQLQuery(query);
			
			if(paramNames != null)
			{
				for(int i = 0; i < paramNames.length; i++)
				{
					q.setParameter(paramNames[i], paramValues[i]);
				}
			}
	
			it = q.list().iterator();
			tx.commit();
		}
		catch(HibernateException e)
		{
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
			}
		}
		
		while ( it.hasNext() ) 
		{
			Object row = it.next();
			
			result.add(row);
		}
		
		return result;
	}

	@SuppressWarnings("unchecked")
	public ArrayList findSql(String query)
	{
		return findSql(query, null, null);
	}

	public boolean delete(Object pojo) 
	{
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
			return false;
		}

		try
		{
			session.delete(pojo);
			
			tx.commit();
			return true;
		}
		catch(HibernateException e)
		{
			e.printStackTrace();
			HibernateUtil3.handleHibernateException(tx, e);
			return false;
		}
		finally
		{
			try
			{
				HibernateUtil3.closeSession();
			}
			catch (HibernateException e2)
			{
			}
		}		
	}
	
}
