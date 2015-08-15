package com.salesforce.migrationtools.metawarrior.metadata;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MetadataTypes extends AbstractModelObject {
	/*private List<MetadataType> m_groups = new ArrayList<MetadataType>();

	public void addType(MetadataType group) {
		List<MetadataType> oldValue = m_groups;
		m_groups = new ArrayList<MetadataType>(m_groups);
		m_groups.add(group);
		firePropertyChange("groups", oldValue, m_groups);
	}

	public void removeType(MetadataType group) {
		List<MetadataType> oldValue = m_groups;
		m_groups = new ArrayList<MetadataType>(m_groups);
		m_groups.remove(group);
		firePropertyChange("groups", oldValue, m_groups);
	}

	public List<MetadataType> getGroups() {
		return m_groups;
	}*/
	
	private List<MetadataType> metadataTypes = new ArrayList<MetadataType>();
	private HashMap<String, MetadataType> metadataTypesMap = new HashMap<String, MetadataType>(); 
	
	public List<MetadataType> getMetadataTypes() {
		return metadataTypes;
	}

	public void setMetadataTypes(ArrayList<MetadataType> metadataTypes) {
		firePropertyChange("metadataTypes", this.metadataTypes, this.metadataTypes = metadataTypes);
		for (MetadataType mt : metadataTypes) {
			metadataTypesMap.put(mt.getTypeName(), mt);
		}
	}

	public MetadataTypes() {
		
	}
	
	public void addType(MetadataType type) {
		metadataTypes.add(type);
	}

	@Override
	  public String toString() {
	    return metadataTypes.toString();
	  }
	
	public MetadataType getMetadataTypeByName(String type) {
		return metadataTypesMap.get(type);
	}
}
