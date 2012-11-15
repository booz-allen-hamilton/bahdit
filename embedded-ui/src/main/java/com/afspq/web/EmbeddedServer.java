package com.afspq.web;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.ProtectionDomain;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

public final class EmbeddedServer {
	public static void main(String[] args) throws Exception {
		int port = Integer.parseInt(System.getProperty("port", "8080"));
		Server server = new Server(port);

		ProtectionDomain domain = EmbeddedServer.class.getProtectionDomain();
		URL location = domain.getCodeSource().getLocation();

		WebAppContext webapp = new WebAppContext();
		webapp.setContextPath("/");
		webapp.setDescriptor(location.toExternalForm() + "/WEB-INF/web.xml");
		webapp.setServer(server);
		webapp.setWar(location.toExternalForm());

		String dir = System.getProperty("dir", "/tmp/bahdit");
		webapp.setTempDirectory(new File(dir));

		server.setHandler(webapp);
		try {
			server.start();
			server.join();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}