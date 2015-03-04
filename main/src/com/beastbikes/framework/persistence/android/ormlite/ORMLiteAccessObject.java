package com.beastbikes.framework.persistence.android.ormlite;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.beastbikes.framework.persistence.DataAccessObject;
import com.beastbikes.framework.persistence.PersistenceException;
import com.beastbikes.framework.persistence.PersistenceManager;
import com.beastbikes.framework.persistence.PersistentObject;
import com.j256.ormlite.dao.BaseDaoImpl;
import com.j256.ormlite.dao.RawRowMapper;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.table.TableInfo;

public class ORMLiteAccessObject<T extends PersistentObject> implements
		DataAccessObject<T>, RawRowMapper<T> {

	private final PersistenceManager persistenceManager;

	private final BaseDaoImpl<T, Serializable> dao;

	private final TableInfo<T, Serializable> tableInfo;

	@SuppressWarnings("unchecked")
	public ORMLiteAccessObject(ORMLitePersistenceSupport support, Class<T> clazz) {
		this.persistenceManager = support;

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
		return this.persistenceManager;
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
	public void insert(T po) throws PersistenceException {
		try {
			this.dao.create(po);
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

	@Override
	public void update(T po) throws PersistenceException {
		try {
			this.dao.update(po);
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

	@Override
	public void delete(T po) throws PersistenceException {
		try {
			this.dao.delete(po);
		} catch (SQLException e) {
			throw new PersistenceException(e);
		}
	}

	@Override
	public void delete(Serializable id) throws PersistenceException {
		try {
			this.dao.deleteById(id);
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
	public void execute(String sql, Object... args) throws PersistenceException {
		final String[] params = new String[args.length];

		for (int i = 0; i < args.length; i++) {
			params[i] = (null == args[i] ? null : String.valueOf(args[i]));
		}

		try {
			this.dao.executeRaw(sql, params);
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

}
