/*
 * Created on 07-Oct-2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package ims.quartz.scheduler.hibernate;

import java.util.ArrayList;

import ims.configuration.InitConfig;

public class HibernateProxy
{
	private String hibernateVersion;

	public HibernateProxy()
	{
		hibernateVersion = InitConfig.getHibernateVersion();	
	}
	
	public ArrayList find(final String query, final String[] paramNames, final Object[] paramValues)
	{
		if(hibernateVersion.equals("3"))
		{
			try
			{
				return ((QueryRunner)Class.forName("ims.quartz.scheduler.hibernate.QueryRunner3").newInstance()).find(query, paramNames, paramValues);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			try
			{
				return ((QueryRunner)Class.forName("ims.quartz.scheduler.hibernate.QueryRunner2").newInstance()).find(query, paramNames, paramValues);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
			
		}
		
		return null;
	}

	public ArrayList find(final String query)
	{
		if(hibernateVersion.equals("3"))
		{
			try
			{
				return ((QueryRunner)Class.forName("ims.quartz.scheduler.hibernate.QueryRunner3").newInstance()).find(query);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			try
			{
				return ((QueryRunner)Class.forName("ims.quartz.scheduler.hibernate.QueryRunner2").newInstance()).find(query);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
	public boolean saveOrUpdate(Object[] pojo)
	{
		if(hibernateVersion.equals("3"))
		{
			try
			{
				return ((QueryRunner)Class.forName("ims.quartz.scheduler.hibernate.QueryRunner3").newInstance()).saveOrUpdate(pojo);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			try
			{
				return ((QueryRunner)Class.forName("ims.quartz.scheduler.hibernate.QueryRunner2").newInstance()).saveOrUpdate(pojo);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}

		return false;
	}
	
	public boolean delete(Object pojo)
	{
		if(hibernateVersion.equals("3"))
		{
			try
			{
				return ((QueryRunner)Class.forName("ims.quartz.scheduler.hibernate.QueryRunner3").newInstance()).delete(pojo);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			try
			{
				return ((QueryRunner)Class.forName("ims.quartz.scheduler.hibernate.QueryRunner2").newInstance()).delete(pojo);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}

		return false;
	}
	
	public Object getDomainObject(Class clazz, int id)
	{
		return getDomainObject(clazz, new Integer(id));
	}
	
	public Object getDomainObject(Class clazz, Integer id)
	{
		if(hibernateVersion.equals("3"))
		{
			try
			{
				return ((QueryRunner)Class.forName("ims.quartz.scheduler.hibernate.QueryRunner3").newInstance()).getDomainObject(clazz, id);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			try
			{
				return ((QueryRunner)Class.forName("ims.quartz.scheduler.hibernate.QueryRunner2").newInstance()).getDomainObject(clazz, id);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}

		return null;
	}
	
	public ArrayList findSql(final String query, final String[] paramNames, final Object[] paramValues)
	{
		if(hibernateVersion.equals("3"))
		{
			try
			{
				return ((QueryRunner)Class.forName("ims.quartz.scheduler.hibernate.QueryRunner3").newInstance()).findSql(query, paramNames, paramValues);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}
		
		return null;
	}

	public ArrayList findSql(final String query)
	{
		if(hibernateVersion.equals("3"))
		{
			try
			{
				return ((QueryRunner)Class.forName("ims.quartz.scheduler.hibernate.QueryRunner3").newInstance()).findSql(query);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
}
