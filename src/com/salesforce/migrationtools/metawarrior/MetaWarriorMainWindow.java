package com.salesforce.migrationtools.metawarrior;

import java.awt.EventQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Properties;

import javax.swing.JFrame;

import net.miginfocom.swing.MigLayout;

import javax.swing.JTabbedPane;
import javax.swing.JPanel;
import javax.swing.JTree;
import javax.swing.JButton;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.salesforce.migrationtools.MetadataLoginUtil;
import com.salesforce.migrationtools.Utils;
import com.salesforce.migrationtools.metawarrior.metadata.MetadataItem;
import com.salesforce.migrationtools.metawarrior.metadata.MetadataType;
import com.salesforce.migrationtools.metawarrior.metadata.MetadataTypes;
import com.salesforce.migrationtools.metawarrior.metadataops.MWConstants;
import com.salesforce.migrationtools.metawarrior.metadataops.MetadataFetcher;
import com.sforce.soap.metadata.MetadataConnection;
import com.sforce.ws.ConnectionException;

import javax.swing.JMenuBar;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeNode;

import org.jdesktop.beansbinding.BeanProperty;

import java.util.List;

import org.jdesktop.beansbinding.ObjectProperty;
import org.jdesktop.beansbinding.AutoBinding;
import org.jdesktop.beansbinding.Bindings;
import org.jdesktop.beansbinding.AutoBinding.UpdateStrategy;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import javax.swing.JTextPane;
import java.awt.BorderLayout;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.RowSpec;
import com.jgoodies.forms.factories.FormFactory;

public class MetaWarriorMainWindow {
	
	private static ArrayList<Properties> orgPropertiesList = new ArrayList<Properties>();
	private static Properties metawarriorProperties;
	
	private static final Logger logger = LogManager.getLogger();

	private JFrame frmMetawarrior;
	
	private static MWProperties props;
	
	private static MetaWarriorController mwController; 
	
	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		
		if (args.length < 1) {
			logger.fatal("Usage parameters: path\\to\\metawarrior\\property\\file\\metawarrior.properties");
			logger.fatal("Example: c:\\temp\\metawarrior\\properties\\metawarrior.properties");
			logger.fatal("Parameter not supplied - exiting.");
			System.exit(0);
		} else {
			// initialize properties
			props = new MWProperties(args[0]);
			
		}
		
		// initialize controller
		
		mwController = new MetaWarriorController(props);
		
		
//		initDummyData();
		
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					MetaWarriorMainWindow window = new MetaWarriorMainWindow();
					window.frmMetawarrior.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	

	/**
	 * Create the application.
	 */
	public MetaWarriorMainWindow() {
		
		// set up the list of properties file that we know of
		
		
		
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frmMetawarrior = new JFrame();
		frmMetawarrior.setTitle("MetaWarrior");
		frmMetawarrior.setBounds(100, 100, 1103, 816);
		frmMetawarrior.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		DefaultMutableTreeNode root = new DefaultMutableTreeNode("dummy");
		DefaultTreeModel treeModel = new DefaultTreeModel(root);
		JMenuBar menuBar = new JMenuBar();
		frmMetawarrior.setJMenuBar(menuBar);
		frmMetawarrior.getContentPane().setLayout(new BorderLayout(0, 0));
		
		JTextPane statusPane = new JTextPane();
		statusPane.setText("Tool started");
		statusPane.setEditable(false);
		frmMetawarrior.getContentPane().add(statusPane, BorderLayout.SOUTH);
		
		JPanel applicationPanel = new JPanel();
		frmMetawarrior.getContentPane().add(applicationPanel, BorderLayout.CENTER);
		applicationPanel.setLayout(new MigLayout("", "[1081px][][grow]", "[733px,grow]"));
		
		JTabbedPane tabbedPane = new JTabbedPane(JTabbedPane.TOP);
		applicationPanel.add(tabbedPane, "cell 0 0,grow");

		createTabsForOrgs(tabbedPane);
		
		JTree tree = new JTree();
		tabbedPane.addTab("New tab", null, tree, null);
		
		JButton btnNewButton = new JButton("New button");
		applicationPanel.add(btnNewButton, "cell 1 0,aligny top");
		
		JPanel panel = new JPanel();
		applicationPanel.add(panel, "cell 2 0,grow");
	}

	/*
	 * This method will add a tab for each org configured
	 */
	
	private void createTabsForOrgs (JTabbedPane tabbedPane) {
		for (String orgName : props.getOrgNames()) {
			String mouseover = mwController.getMouseOver(orgName);
			DefaultTreeModel treeModel = getTreeModelForOrg(orgName);
			JTree metadataTree = new JTree(treeModel);
			JScrollPane scrollPane = new JScrollPane(metadataTree, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED,
				      JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
			tabbedPane.addTab(orgName, null, scrollPane, mouseover);
		}
	}
	
	private DefaultTreeModel getTreeModelForOrg(String orgName) {
		DefaultMutableTreeNode root = new DefaultMutableTreeNode(orgName);
		DefaultTreeModel myModel = new DefaultTreeModel(root);
		
		MetadataTypes types = mwController.getMDTypesHash(orgName);
		
		Collections.sort(types.getMetadataTypes(), new Comparator<MetadataType>() {
			@Override
			
			public int compare (final MetadataType md1, final MetadataType md2) {
				return md1.getTypeName().compareTo(md2.getTypeName());
			}
		});  
		
		for (MetadataType type : types.getMetadataTypes()) {
			DefaultMutableTreeNode mdTypeNode = new DefaultMutableTreeNode(type.getTypeName() + "(" + type.getItemCount() + ")");
			root.add(mdTypeNode);
			for (MetadataItem item : type.getMetadataItems()) {
				mdTypeNode.add(new DefaultMutableTreeNode(item.getItemName()));
			}
		}
		return myModel;
	}
	
	private static MetadataTypes initDummyData(String orgName) {
		MetadataTypes types = new MetadataTypes();
		MetadataType type = new MetadataType();
		type.setTypeName("Objects" + " - " + orgName);
		types.addType(type);
		type.addItem(new MetadataItem("Object1" + " - " + orgName));
		type.addItem(new MetadataItem("Object2" + " - " + orgName));
		type.addItem(new MetadataItem("Object3" + " - " + orgName));
		type = new MetadataType();
		type.setTypeName("Pages");
		types.addType(type);
		type.addItem(new MetadataItem("Page1" + " - " + orgName));
		type.addItem(new MetadataItem("Page2" + " - " + orgName));
		type.addItem(new MetadataItem("Page3" + " - " + orgName));
		return types;
	}

}
