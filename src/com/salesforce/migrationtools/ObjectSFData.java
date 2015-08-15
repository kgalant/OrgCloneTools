package com.salesforce.migrationtools;

import java.io.IOException;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.salesforce.migrationtools.dbinventory.DBOps;
import com.sforce.soap.metadata.CustomObject;
import com.sforce.soap.metadata.Metadata;
import com.sforce.soap.partner.ActionOverride;
import com.sforce.soap.partner.DescribeSObjectResult;

public class ObjectSFData {

	private static final Logger logger = LogManager.getLogger("MyLogger");

	private CustomObject myMetadata;
	private DescribeSObjectResult myDescribe;
	private JsonNode myJson;

	private String myOrgId;

	public String getMyOrgId() {
		return myOrgId;
	}

	public void setMyOrgId(String myOrgId) {
		this.myOrgId = myOrgId;
	}

	public ObjectSFData(String orgId, Metadata md, DescribeSObjectResult desc) {
		myOrgId = orgId;
		addMetadataInfo(md);
		addDescribeInfo(desc);
		myJson = getCombinedJson();

	}

	public JsonNode getMyJson() {
		return myJson;
	}

	public boolean storeInDB() {
		return DBOps.storeObjectDataInDB(this);

	}

	public void addMetadataInfo(Metadata md) {
		myMetadata = (CustomObject) md;
	}

	public void addDescribeInfo(DescribeSObjectResult desc) {
		myDescribe = desc;
	}

	public boolean getActivateable() {
		if (myDescribe != null)
			return myDescribe.getActivateable();
		return false;
	}

	public ActionOverride[] getActionOverrides() {
		if (myDescribe != null)
			return myDescribe.getActionOverrides();
		return null;
	}

	public boolean getAllowInChatterGroups() {
		if (myMetadata != null)
			return myMetadata.getAllowInChatterGroups();
		return false;
	}

	public String getName() {
		if (myMetadata != null) {
			return myMetadata.getFullName();
		} else {
			return myDescribe.getName();
		}
	}

	public String getMetadataAsJSONString() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(myMetadata);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			return null;
		}
	}

	public String getDescribeAsJSONString() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(myDescribe);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			return null;
		}
	}

	public JsonNode getMetadataAsJSON() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(mapper.writeValueAsString(myMetadata), JsonNode.class);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			return null;
		} catch (IOException e) {
			logger.error(e);
			return null;
		}
	}

	public JsonNode getDescribeAsJSON() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(mapper.writeValueAsString(myDescribe), JsonNode.class);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			return null;
		} catch (IOException e) {
			logger.error(e);
			return null;
		}

	}

	public String toJSON() {
		StringBuffer retval = new StringBuffer();
		retval.append(getMetadataAsJSONString());
		retval.append(getDescribeAsJSONString());
		return retval.toString();

	}

	private JsonNode getCombinedJson() {
		
		
		JsonNode rootNode = merge(getMetadataAsJSON(), getDescribeAsJSON());
		System.err.println(rootNode.toString());
		return rootNode;
	}

	public static JsonNode merge(JsonNode mainNode, JsonNode updateNode) {

		Iterator<String> fieldNames = updateNode.fieldNames();
		while (fieldNames.hasNext()) {

			String fieldName = fieldNames.next();
			JsonNode jsonNode = mainNode.get(fieldName);
			// if field exists and is an embedded object
			if (jsonNode != null && jsonNode.isObject()) {
				merge(jsonNode, updateNode.get(fieldName));
			} else {
				if (mainNode instanceof ObjectNode) {
					// Overwrite field
					JsonNode value = updateNode.get(fieldName);
					((ObjectNode) mainNode).put(fieldName, value);
				}
			}

		}

		return mainNode;
	}
}
