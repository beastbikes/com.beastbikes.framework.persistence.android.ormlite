package com.beastbikes.framework.persistence.android.ormlite;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import android.text.TextUtils;

import com.beastbikes.framework.persistence.DataAccessObject;
import com.beastbikes.framework.persistence.PersistenceException;
import com.beastbikes.framework.persistence.PersistenceManager;
import com.beastbikes.framework.persistence.PersistentObject;
import com.j256.ormlite.dao.BaseDaoImpl;
import com.j256.ormlite.dao.CloseableIterator;
import com.j256.ormlite.dao.GenericRawResults;
import com.j256.ormlite.dao.RawRowMapper;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.misc.TransactionManager;
import com.j256.ormlite.table.TableInfo;

public class ORMLiteAccessObject<T extends PersistentObject> implements
		DataAccessObject<T>, RawRowMapper<T> {

	private final ORMLitePersistenceSupport support;

	private final BaseDaoImpl<T, Serializable> dao;

	private final TableInfo<T, Serializable> tableInfo;

	@SuppressWarnings("unchecked")
	public ORMLiteAccessObject(ORMLitePersistenceSupport support, Class<T> clazz) {
		this.support = support;

		try {
			this.dao = (BaseDaoImpl<T, Serializable>) support.getDao(clazz);
			this.tableInfo = new TableInfo<T, Serializable>(
					support.getConnectionSource(), this.dao, clazz);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		return this.support;
	}

	@Override
	public long count() throws PersistenceException {
		try {
			return this.dao.countOf();
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

	@Override
	public long count(String clauses, String... args)
			throws PersistenceException {
		final String sql = new StringBuilder("SELECT COUNT(*) FROM ")
				.append(this.tableInfo.getTableName()).append(" WHERE ")
				.append(clauses).toString();
		try {
			final GenericRawResults<String[]> results = this.dao.queryRaw(sql,
					args);
			if (null == results)
				return 0;

			try {
				return Long.parseLong(results.getFirstResult()[0]);
			} finally {
				results.close();
			}
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

	@Override
	public T get(Serializable id) throws PersistenceException {
		try {
			return this.dao.queryForId(id);
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

	@Override
	public List<T> getAll() throws PersistenceException {
		try {
			return this.dao.queryForAll();
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

	@Override
	public void insert(final T... pos) throws PersistenceException {
		if (null == pos || pos.length < 0)
			return;

		this.execute(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				for (final T t : pos) {
					ORMLiteAccessObject.this.dao.create(t);
				}

				return null;
			}

		});
	}

	@Override
	public void update(final T... pos) throws PersistenceException {
		if (null == pos || pos.length < 0)
			return;

		this.execute(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				for (final T t : pos) {
					ORMLiteAccessObject.this.dao.update(t);
				}

				return null;
			}

		});
	}

	@Override
	public void delete(final T... pos) throws PersistenceException {
		if (null == pos || pos.length < 0)
			return;

		this.execute(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				for (final T t : pos) {
					ORMLiteAccessObject.this.dao.delete(t);
				}

				return null;
			}

		});
	}

	@Override
	public void delete(Serializable... ids) throws PersistenceException {
		try {
			this.dao.deleteIds(Arrays.asList(ids));
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

	@Override
	public boolean exists(T po) throws PersistenceException {
		try {
			return this.dao.idExists(po.getId());
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

	@Override
	public boolean exists(Serializable id) throws PersistenceException {
		try {
			return this.dao.idExists(id);
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

	@Override
	public void execute(String sql, String... args) throws PersistenceException {
		try {
			this.dao.executeRaw(sql, args);
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

	@Override
	public T mapRow(String[] names, String[] values) throws SQLException {
		final FieldType[] fts = this.tableInfo.getFieldTypes();
		final T t = this.tableInfo.createObject();
		final int n = Math.min(names.length, values.length);
		final Map<String, String> row = new HashMap<String, String>();

		for (int i = 0; i < n; i++) {
			row.put(names[i], values[i]);
		}

		for (int i = 0; i < fts.length; i++) {
			final FieldType ft = fts[i];
			final Field f = ft.getField();
			final String v = row.get(ft.getColumnName());

			if (null == v)
				continue;

			try {
				f.setAccessible(true);
				f.set(t, ft.convertStringToJavaField(v, i));
			} catch (Exception e) {
				throw new SQLException(e);
			}
		}

		return t;
	}

	@Override
	public <V> V execute(final Callable<V> transaction)
			throws PersistenceException {
		try {
			return TransactionManager.callInTransaction(
					this.support.getConnectionSource(), transaction);
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

	public List<T> query(String clauses, String... args)
			throws PersistenceException {
		final StringBuilder sql = new StringBuilder("SELECT * FROM ");

		sql.append(this.tableInfo.getTableName());

		if (!TextUtils.isEmpty(clauses)) {
			if (!clauses.startsWith(" ")) {
				sql.append(" ");
			}
			sql.append(clauses);
		}

		try {
			final GenericRawResults<T> results = this.dao.queryRaw(
					sql.toString(), this, args);
			if (results == null)
				return null;

			final List<T> list = new ArrayList<T>();
			final CloseableIterator<T> i = results.closeableIterator();

			try {
				while (i.hasNext()) {
					list.add(i.nextThrow());
				}
			} finally {
				i.close();
			}

			return list;
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

}
