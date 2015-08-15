package com.salesforce.migrationtools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class PackageMerge {

	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException, TransformerException {
		
		if (args.length < 2) {
			System.out.println("Usage: java -jar PackageMerge.jar outputfilename inputfile1 [inputfile2] [inputfile3] ...");
			System.out.println("Will merge any input files provided into outputfilename, retaining inputfile1's version setting.");
			System.exit(0);
		}
		
		File outputFile = new File(args[0]);

		mergePackageFiles(outputFile, args);

	}

	public static void mergePackageFiles(File outputFile, String[] args) throws IOException, ParserConfigurationException, SAXException, TransformerException {

		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		
		ArrayList<HashMap<String, HashSet<String>>> maps = new ArrayList<HashMap<String, HashSet<String>>>();
		Document doc1 = null;
		for (int i = 1; i < args.length; i++) { // skipping args[0] since that's the output file name
			File fileToParse = new File(args[i]);
			
			if (fileToParse != null && fileToParse.exists()) {
				Document doc;
				try {
					doc = dBuilder.parse(fileToParse);
					maps.add(convertXmlToMetadataHash(doc));
					if (i==1) {
						doc1=doc;
					}
				} catch (Exception e) {
					System.out.println("Error parsing file " + args[i] + ", skipping...");
					e.printStackTrace();
				}
				
			} else {
				System.out.println("File " + args[i] + " not found or can't be opened, skipping...");
			}
		}
		
		// join the collections

		HashMap<String, HashSet<String>> resultPackageHash = joinHashmaps(maps);

		// produce the output document

		Element packageElement = doc1.getDocumentElement();
		Element versionElement = (Element) packageElement.getElementsByTagName("version").item(0);

		Document output = dBuilder.newDocument();
		Node rootNode = output.importNode(doc1.getDocumentElement(), false);
		output.appendChild(rootNode);
		Element outputPackageElement = output.getDocumentElement();

		// add version element
		Node versionNode = output.importNode(versionElement, true);
		Element outputVersionElement = (Element) versionNode;
		outputPackageElement.appendChild(outputVersionElement);

		for (String mdTypeName : new ArrayList<String>(new TreeSet<String>(resultPackageHash.keySet()))) {
			// create types element

			Element typesElement = output.createElement("types");
			outputPackageElement.appendChild(typesElement);
			Element nameElement = output.createElement("name");
			nameElement.setTextContent(mdTypeName);
			typesElement.appendChild(nameElement);

			HashSet<String> itemsList = resultPackageHash.get(mdTypeName);
			for (String itemName : new ArrayList<String>(new TreeSet<String>(itemsList))) {
				// create element
				Element membersElement = output.createElement("members");
				membersElement.setTextContent(itemName);
				typesElement.appendChild(membersElement);
			}

		}

		// write the content into xml file
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
		DOMSource source = new DOMSource(output);
		StreamResult result = new StreamResult(outputFile);

		// Output to console for testing
		// StreamResult result = new StreamResult(System.out);

		transformer.transform(source, result);

	}

	private static HashMap<String, HashSet<String>> convertXmlToMetadataHash(Document xml) {

		HashMap<String, HashSet<String>> retval = new HashMap<String, HashSet<String>>();

		NodeList typesList = xml.getElementsByTagName("types");

		for (int i = 0; i < typesList.getLength(); i++) {

			Element mdTypeElement = (Element) typesList.item(i);
			String mdTypeName = mdTypeElement.getElementsByTagName("name").item(0).getTextContent();

			System.out.println("Processing metadata type: " + mdTypeName);

			HashSet<String> itemSet = null;

			if (retval.get(mdTypeName) != null) {
				itemSet = retval.get(mdTypeName);
			}

			if (itemSet == null) {
				itemSet = new HashSet<String>();
				retval.put(mdTypeName, itemSet);
			}

			NodeList memberList = mdTypeElement.getElementsByTagName("members");

			for (int j = 0; j < memberList.getLength(); j++) {
				Element memberElement = (Element) memberList.item(j);
				itemSet.add(memberElement.getTextContent());
				System.out.println("Found item: " + memberElement.getTextContent());
			}
		}

		return retval;

	}

	private static HashMap<String, HashSet<String>> joinHashmaps(ArrayList<HashMap<String, HashSet<String>>> maps) {
		HashMap<String, HashSet<String>> retval = new HashMap<String, HashSet<String>>();

		for (HashMap<String, HashSet<String>> map : maps) {
			for (String mdType : map.keySet()) {
				HashSet<String> targetItemSet = getItemSet(retval, mdType);
				HashSet<String> sourceItemSet = getItemSet(map, mdType);
				targetItemSet.addAll(sourceItemSet);
			}
		}

		return retval;
	}

	private static HashSet<String> getItemSet(HashMap<String, HashSet<String>> map, String typeName) {
		HashSet<String> itemSet = null;

		if (map.get(typeName) != null) {
			itemSet = map.get(typeName);
		} else {
			itemSet = new HashSet<String>();
			map.put(typeName, itemSet);
		}
		return itemSet;
	}
}
