package jcr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.TimeZone;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.commons.JcrUtils;

public class Jcr {

	private static final String PROGRAMMES_FOLDER_NAME = "programmes";
	
	public byte[] getPdf(Long id, LocalDateTime localDateTime) {
		return readData(PROGRAMMES_FOLDER_NAME + "/" + id, localDateTime);
	}	

	public void uploadPdf(Long id, 	byte[] fileData) {
		uploadFile(PROGRAMMES_FOLDER_NAME, String.valueOf(id), fileData);  	   
	}
	
	private byte[] readData(String folderName, LocalDateTime localDateTime)	{
		Session session = initSession();
		localDateTime = localDateTime.minusHours(3);
		try { 
			String queryS = "select * from [nt:resource] as file where( "
		    				+ "ISDESCENDANTNODE(file,[/" + folderName + "/])"
		    				+ " and "
		    				+ "[jcr:lastModified] < "
		    				+ "CAST('" + localDateTime.toString() + "' AS DATE)) "
		    				+ "order by [jcr:lastModified] desc";

		    QueryManager queryManager = session.getWorkspace().getQueryManager();
		    Query query = queryManager.createQuery(queryS, Query.JCR_SQL2);
		    query.setLimit(1);
		    QueryResult result = query.execute();
			Node file = result.getNodes().nextNode();
			
			return IOUtils.toByteArray(JcrUtils.readFile(file));
			
		} catch (IOException | RepositoryException e) {
			e.printStackTrace();
		} finally {
			session.logout();
		}
		return null;
	}
	
	private void uploadFile(String folderName, String fileName, byte[] data) {
		Session session = initSession();
		try {
			InputStream stream = new ByteArrayInputStream(data);	        
			
        	Node folderRoot = JcrUtils.getOrAddFolder(session.getRootNode(), folderName);
        	
        	Node folder = JcrUtils.getOrAddFolder(folderRoot, fileName);
        	
        	Calendar created = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        	
        	fileName = fileName + "_" + created.getTimeInMillis();
        	
	        JcrUtils.putFile(folder, fileName, "mix:title", stream, created);	        
	        session.save();
	        
		} catch (RepositoryException e) {
	    	e.printStackTrace();
		} finally {
			session.logout();
		}
	}
    	
    private Session initSession() {    	
    	try {
            InitialContext context = new InitialContext();
            Repository repository = (Repository)context.lookup("jcr/local");
            
            return repository.login(new SimpleCredentials("jackrabbit", "jackrabbit".toCharArray()));
            
   		} catch (RepositoryException | NamingException e) {
    		e.printStackTrace();
    	}
    	return null;
    }
}
