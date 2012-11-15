package com.bah.applefox.main.plugins.webcrawler.utilities;

import java.util.ArrayList;
import java.util.List;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * SAX handler that attempts to find paragraphs. The only clear indicator of a
 * paragraph in the XHTML spec is the &lt;p&gt; tag, so we will use these tags
 * to break on individual paragraphs. If other methods are used to break
 * paragraph structure, the document content will be interpreted as one long
 * paragraph.
 */
public class ParagraphContentHandler extends DefaultHandler {

	private StringBuilder builder = new StringBuilder();
	private List<String> paragraphs = new ArrayList<String>();
	
	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if(localName.equals("p")){
			breakParagraph();
		}
	}
	
	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if(localName.equals("p")){
			breakParagraph();
		}
	}
	
	@Override
	public void endDocument() throws SAXException {
		breakParagraph();
	}
	
	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		builder.append(ch, start, length);
	}
	
	private void breakParagraph(){
		if(builder.length() == 0){
			return;
		}
		String builtString = builder.toString();
		builtString = builtString.trim().replaceAll("\\s+", " ");
		// replace the builder before any possible return statement, but after
		// we have extracted the information from it.
		builder = new StringBuilder();
		if(builtString.length()==0){
			return;
		}
		paragraphs.add(builtString);
	}
	
	public List<String> getParagraphs() {
		return paragraphs;
	}
}
