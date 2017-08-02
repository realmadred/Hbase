package com.hbase.entity;

import java.io.Serializable;
import java.util.Map;

/**
 * 多个列族数据
 */
public class HbaseDataFamilys implements Serializable{

	private static final long serialVersionUID = 4451482729334530119L;
	private String key;
	private Map<String,Map<String,String>> columns;//map<columnfamily,map<column,value>>
	
	public HbaseDataFamilys() {
		super();
	}
	
	public HbaseDataFamilys(String tableName, String nameSpace,
							String key, Map<String, Map<String, String>> columns) {
		super();
		this.key = key;
		this.columns = columns;
	}
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Map<String, Map<String, String>> getColumns() {
		return columns;
	}
	public void setColumns(Map<String, Map<String, String>> columns) {
		this.columns = columns;
	}

	@Override
	public String toString() {
		return "HbaseDataFamilys{" +
				", key='" + key + '\'' +
				", columns=" + columns +
				'}';
	}
}

