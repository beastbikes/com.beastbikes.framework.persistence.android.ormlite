package com.beastbikes.framework.persistence.android.ormlite;

import java.util.Arrays;
import java.util.Comparator;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDatabase.CursorFactory;

import com.beastbikes.framework.persistence.android.SQLiteUpgradeHandler;
import com.j256.ormlite.android.apptools.OrmLiteSqliteOpenHelper;
import com.j256.ormlite.support.ConnectionSource;

/**
 * ORMLite implementation of interface {@link ORMLitePersistenceManager}
 * 
 * @author johnson
 * 
 */
public abstract class ORMLitePersistenceSupport extends OrmLiteSqliteOpenHelper
		implements Comparator<SQLiteUpgradeHandler>, ORMLitePersistenceManager {

	public ORMLitePersistenceSupport(Context context, String name,
			CursorFactory factory, int version) {
		super(context, name, factory, version);
	}

	@Override
	public SQLiteUpgradeHandler[] getUpgradeHandlers() {
		return new SQLiteUpgradeHandler[0];
	}

	@Override
	public int compare(SQLiteUpgradeHandler lhs, SQLiteUpgradeHandler rhs) {
		return lhs.compareTo(rhs);
	}

	@Override
	public void onUpgrade(SQLiteDatabase db, ConnectionSource cs,
			int oldVersion, int newVersion) {
		final SQLiteUpgradeHandler[] handlers = getUpgradeHandlers();
		if (null == handlers || handlers.length <= 0)
			return;

		Arrays.sort(handlers, this);

		for (int i = 0; i < handlers.length; i++) {
			final SQLiteUpgradeHandler handler = handlers[i];
			final int targetVersion = handler.getTargetVersion();

			if (oldVersion < targetVersion) {
				handler.upgrade(this, oldVersion, newVersion);
			}
		}
	}

}
