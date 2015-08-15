package com.salesforce.migrationtools.metawarrior.metadata;

public class MetadataItem extends AbstractModelObject {

	private String itemName;

	public MetadataItem() {
		
	}
	
	public MetadataItem(String itemName) {
		setItemName(itemName);
	}

	public String getItemName() {
		return itemName;
	}

	public void setItemName(String itemName) {
		String oldValue = itemName;
		this.itemName = itemName;
		firePropertyChange("itemName", oldValue,this.itemName);
	}
	
	public String toString() {
		return itemName;
	}

}
