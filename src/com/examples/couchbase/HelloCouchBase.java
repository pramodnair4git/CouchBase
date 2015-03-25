package com.examples.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewDesign;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;

public class HelloCouchBase {

	private CouchbaseClient client = null;

	public HelloCouchBase() {
		//System.setProperty("viewmode", "development");
	}

	public void connect() {
		List<URI> nodes = new ArrayList<URI>();
		nodes.add(URI.create("http://localhost:8091/pools"));

		try {
			client = new CouchbaseClient(nodes, "beer", "");
		} catch (IOException e) {
			System.out.println("Couldn't connect to couchbase server : "
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	public void createDocument() {
		if (client != null) {
			client.add("user", "couchbase");
			client.add("version", "3.0");
			System.out.println("Hello " + client.get("user") + " "
					+ client.get("version"));
		} else
			System.out.println("Couchbase connection not established!!");
	}

	public void shutDownCouchbase() {
		client.shutdown();
	}

	public void operatOnViews(String designDoc, String viewName) {
		// View view = client.getView("players", "leaderboard");
		View view = client.getView(designDoc, viewName);
		Query query = new Query();
		query.setIncludeDocs(true);
		query.setDebug(true);
		ViewResponse response = client.query(view, query);
		for (ViewRow viewRow : response) {
			System.out.println(viewRow.getDocument());
		}
	}

	public void createViews() {
		DesignDocument designDoc = new DesignDocument("dev_beer4");
		String viewName = "beer_view";
		/*String mapFunction = "map(doc, meta){\n"
				+ "if(doc.type && doc.type == \"json\"){\n" + " emit(doc);\n"
				+ "}\n" + "}";*/
		String mapFunction =

	            "function (doc, meta) {\n" +

	            "  if(doc.type && doc.type == \"beer\") {\n" +

	            "    emit(doc.name);\n" +

	            "  }\n" +

	            "}";

		
		ViewDesign viewDesign = new ViewDesign(viewName, mapFunction);
		designDoc.getViews().add(viewDesign);
		
		System.out.println(client.createDesignDoc(designDoc));
	}

	public static void main(String[] args) {
		HelloCouchBase server = new HelloCouchBase();
		server.connect();
		server.createDocument();
		server.createViews();
		server.operatOnViews("dev_beer4", "beer_view");
		server.shutDownCouchbase();
	}

}
