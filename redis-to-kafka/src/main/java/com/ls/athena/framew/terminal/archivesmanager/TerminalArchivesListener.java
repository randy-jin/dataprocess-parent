package com.ls.athena.framew.terminal.archivesmanager;

import com.ls.pf.base.api.cache.ICacheListener;

public class TerminalArchivesListener implements ICacheListener{
	public void addEvent(String key, Object value) {
	}

	public void expireEvent(String key, Object value) {
		TerminalArchives.getInstance().refreshCache(key);
	}

	public void removeEvent(String key, Object value) {
	}

}