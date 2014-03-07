package com.nflabs.zeppelin.server;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

import org.apache.cxf.jaxrs.servlet.CXFNonSpringJaxrsServlet;
import org.apache.hadoop.fs.FileSystem;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nflabs.zeppelin.conf.ZeppelinConfiguration;
import com.nflabs.zeppelin.conf.ZeppelinConfiguration.ConfVars;
import com.nflabs.zeppelin.rest.ZANRestApi;
import com.nflabs.zeppelin.rest.ZQLRestApi;
import com.nflabs.zeppelin.scheduler.SchedulerFactory;
import com.nflabs.zeppelin.zan.ZAN;
import com.nflabs.zeppelin.zengine.ZException;
import com.nflabs.zeppelin.zengine.Zengine;


public class ZeppelinServer extends Application {
	private static final Logger LOG = LoggerFactory.getLogger(ZeppelinServer.class);

	private static Zengine z;

	private SchedulerFactory schedulerFactory;
	private ZQLJobManager analyzeSessionManager;
	private ZANJobManager zanJobManager;
	private ZAN zan;

	public static void main(String [] args) throws Exception{
		z = new Zengine();
		ZeppelinConfiguration conf = z.getConf();

		int port = conf.getInt(ConfVars.ZEPPELIN_PORT);
        final Server server = setupJettyServer(port);

        //REST api
		final ServletContextHandler restApi = setupRestApiContextHandler();
		/** NOTE: Swagger is included via the web.xml in zeppelin-web
		 *  A future work: remove web.xml and use this class in order to instenciate all the servlet
		 *  final ServletContextHandler swagger = setupSwaggerContextHandler();
		 */
		//Web UI
		final WebAppContext webApp = setupWebAppContext(conf);

        // add all handlers
	    ContextHandlerCollection contexts = new ContextHandlerCollection();
	    contexts.setHandlers(new Handler[]{restApi, webApp});
	    server.setHandler(contexts);

	    LOG.info("Start zeppelin server");
        server.start();
        LOG.info("Started");

		Runtime.getRuntime().addShutdownHook(new Thread(){
		    @Override public void run() {
		        LOG.info("Shutting down Zeppelin Server ... ");
            	try {
					server.stop();
				} catch (Exception e) {
					LOG.error("Error while stopping servlet container", e);
				}
            	LOG.info("Bye");
            }
        });
		server.join();
	}

    private static Server setupJettyServer(int port) {
        int timeout = 1000*30;
        final Server server = new Server();
        SocketConnector connector = new SocketConnector();

        // Set some timeout options to make debugging easier.
        connector.setMaxIdleTime(timeout);
        connector.setSoLingerTime(-1);
        connector.setPort(port);
        server.addConnector(connector);
        return server;
    }

    private static ServletContextHandler setupRestApiContextHandler() {
        final ServletHolder cxfServletHolder = new ServletHolder( new CXFNonSpringJaxrsServlet() );
		cxfServletHolder.setInitParameter("javax.ws.rs.Application", ZeppelinServer.class.getName());
		cxfServletHolder.setName("rest");
		cxfServletHolder.setForcedPath("rest");

		final ServletContextHandler cxfContext = new ServletContextHandler();
		cxfContext.setSessionHandler(new SessionHandler());
		cxfContext.setContextPath("/cxf");
		cxfContext.addServlet( cxfServletHolder, "/zeppelin/*" );
        return cxfContext;
    }

    /**
     * Swagger core handler - Needed for the RestFul api documentation
     *
     * THIS IS HERE AS FOUNDATION FOR A FUTURE UPDATE
     *
     * @return ServletContextHandler of Swagger
     */
    @Deprecated
    private static ServletContextHandler setupSwaggerContextHandler() {
      final ServletHolder SwaggerSerlet = new ServletHolder(new com.wordnik.swagger.jaxrs.config.DefaultJaxrsConfig());

      SwaggerSerlet.setInitParameter("api.version", "1.0.0");
      SwaggerSerlet.setInitParameter("swagger.api.basepath", "http://localhost:8080/cxf/zeppelin");

      final ServletContextHandler handler = new ServletContextHandler();
      // Setup the handler
      handler.setSessionHandler(new SessionHandler());
      handler.setInitParameter("com.sun.jersey.config.property.packages", "com.nflabs.zeppelin.rest;com.wordnik.swagger.jaxrs.listing");
      handler.setInitParameter("com.sun.jersey.spi.container.ContainerRequestFilters", "com.sun.jersey.api.container.filter.PostReplaceFilter");
      handler.addServlet(SwaggerSerlet, "/api-docs/*");

      return handler;
    }

    private static WebAppContext setupWebAppContext(ZeppelinConfiguration conf) {
        WebAppContext webApp = new WebAppContext();
        File webapp = new File(conf.getString(ConfVars.ZEPPELIN_WAR));

        if(webapp.isDirectory()){ // Development mode, read from FS
            webApp.setDescriptor(webapp+"/WEB-INF/web.xml");
            webApp.setResourceBase(webapp.getPath());
            webApp.setContextPath("/");
            webApp.setParentLoaderPriority(true);
        } else { //use packaged WAR
            webApp.setWar(webapp.getAbsolutePath());
        }
        return webApp;
    }

	public ZeppelinServer() throws Exception {
		this.schedulerFactory = new SchedulerFactory();

		FileSystem fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());

        if(z.useFifoJobScheduler()){
			this.analyzeSessionManager = new ZQLJobManager(z, fs, schedulerFactory.createOrGetFIFOScheduler("analyze"), z.getConf().getString(ConfVars.ZEPPELIN_JOB_DIR));
		} else {
			this.analyzeSessionManager = new ZQLJobManager(z, fs, schedulerFactory.createOrGetParallelScheduler("analyze", 100), z.getConf().getString(ConfVars.ZEPPELIN_JOB_DIR));
		}

		this.zan = new ZAN(z.getConf().getString(ConfVars.ZEPPELIN_ZAN_REPO),
		                   z.getConf().getString(ConfVars.ZEPPELIN_ZAN_LOCAL_REPO),
		                   z.getConf().getString(ConfVars.ZEPPELIN_ZAN_SHARED_REPO),
		                   fs);

		this.zanJobManager = new ZANJobManager(zan, schedulerFactory.createOrGetFIFOScheduler("analyze"));
	}

	@Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> classes = new HashSet<Class<?>>();
        return classes;
    }

	@Override
    public java.util.Set<java.lang.Object> getSingletons(){
    	Set<Object> singletons = new HashSet<Object>();

    	ZQLRestApi analyze = new ZQLRestApi(this.analyzeSessionManager);
    	singletons.add(analyze);

    	ZANRestApi zan = new ZANRestApi(this.zan, this.zanJobManager);
    	singletons.add(zan);

    	return singletons;
    }

}
