package com.salesforce.migrationtools.dbinventory;

import java.util.HashMap;

public class ObjectData {
	private String objectName;
	private String orgId;
	private HashMap<String,String> paramMap;
	public String getObjectName() {
		return objectName;
	}
	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}
	public String getOrgId() {
		return orgId;
	}
	public void setOrgId(String orgId) {
		this.orgId = orgId;
	}
	
	public ObjectData(String orgId, String objName) {
		objectName = objName;
		this.orgId = orgId;
		paramMap = new HashMap<String,String>();
	}
	
	public ObjectData() {
		paramMap = new HashMap<String,String>();
	}
	
	public void addParamValuePair(String key, String value) {
		paramMap.put(key, value);
	}
	
	public String getParam(String paramName) {
		return paramMap.get(paramName);
	}
	
	
}
