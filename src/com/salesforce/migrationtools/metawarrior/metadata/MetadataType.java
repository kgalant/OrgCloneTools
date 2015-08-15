package com.salesforce.migrationtools.metawarrior.metadata;

import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.List;

import com.sforce.soap.metadata.DescribeMetadataObject;

public class MetadataType extends AbstractModelObject {
	private String typeName;
	private ArrayList<MetadataItem> metadataItems = new ArrayList<MetadataItem>();
	private DescribeMetadataObject myDMO;
	private int itemCount;
	
	public ArrayList<MetadataItem> getMetadataItems() {
		return metadataItems;
	}

	public void setMetadataItems(ArrayList<MetadataItem> metadataItems) {
		this.metadataItems = metadataItems;
	}
	
	public MetadataType() {
		
	}

	public MetadataType(DescribeMetadataObject dmo) {
		myDMO = dmo;
		setTypeName(dmo.getXmlName());
	}
	
	public String getTypeName() {
		return typeName;
	}
	
	public int getItemCount() {
		return itemCount;
	}

	public void setTypeName(String typeName) {
		firePropertyChange("typeName", this.typeName, this.typeName = typeName);
	}
	
	public void setItemCount(int itemCount) {
		firePropertyChange("itemCount", this.itemCount, this.itemCount = itemCount);
	}
	
	public void addItem(MetadataItem item) {
		List<MetadataItem> oldItems = metadataItems;
		metadataItems = new ArrayList<MetadataItem>(metadataItems);
		metadataItems.add(item);
		firePropertyChange("metadataItems", oldItems, metadataItems);
	}
	
	public DescribeMetadataObject getDMO() {
		return myDMO;
	}
	
	
	@Override
	  public String toString() {
	    return typeName;
	  }

}
