package com.bah.applefox.main.plugins.webcrawler.utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Objects;
import com.google.common.collect.MapMaker;

/**
 * Checks if
 */
public class RobotsTXT {

	private static final Log LOG = LogFactory.getFactory().getInstance(
			RobotsTXT.class);

	private static enum RuleType {
		ALLOW, DISALLOW, USER_AGENT, ALLOW_ALL, DISALLOW_ALL, USER_AGENT_ALL, CRAWL_DELAY, SITEMAP;
	}

	private static final class Rule {
		final RuleType type;
		final String scope;

		public Rule(RuleType type, String scope) {
			super();
			this.type = type;
			this.scope = scope;
		}
	}

	private static final class UrlAndAgent {
		public UrlAndAgent(String url, String agent) {
			this.agent = agent;
			this.url = url;
		}

		final String agent;
		final String url;

		@Override
		public String toString() {
			return Objects.toStringHelper(RobotsTXT.class).add("agent", agent)
					.add("url", url).toString();
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(agent, url);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof UrlAndAgent) {
				return Objects.equal(agent, ((UrlAndAgent) obj).agent)
						&& Objects.equal(url, ((UrlAndAgent) obj).url);
			}
			return false;
		}
	}

	private final List<Rule> rules;
	private final static Map<UrlAndAgent, RobotsTXT> CACHE = new MapMaker()
			.concurrencyLevel(100).softValues().makeMap();
	private final long expTime;
	private final Pattern matchPattern = Pattern
			.compile(
					"(User-agent|Disallow|Allow|Crawl-delay|Sitemap)\\s*:\\s*(\\S+)\\s*",
					Pattern.CASE_INSENSITIVE);
	private final Pattern scopePattern = Pattern.compile("%[a-fA-F0-9]{2}");
	private final String agent;

	private RobotsTXT(URLConnection con, String agent, long expTime)
			throws PageCrawlException {
		this.agent = agent;
		this.expTime = expTime;
		// Tries to match the character set of the Web Page
		String charset;
		String contentType = con.getContentType();
		if (contentType != null) {
			Matcher m = Pattern.compile("\\s+charset=([^\\s]+)\\s*").matcher(
					contentType);
			charset = m.matches() ? m.group(1) : "utf-8";
		} else {
			charset = "utf-8";
		}
		// once we know the character set, read the rules
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(
					con.getInputStream(), charset));
			String line = null;
			List<Rule> rules = new ArrayList<Rule>();
			while ((line = reader.readLine()) != null) {
				Matcher lineMatcher = matchPattern.matcher(line);
				if (lineMatcher.matches()) {
					String typeString = lineMatcher.group(1).toLowerCase();
					RuleType ruleType;
					String scope = lineMatcher.group(2);
					if (scope.equals("*")) {
						if (typeString.equals("user-agent")) {
							ruleType = RuleType.USER_AGENT_ALL;
						} else if (typeString.equals("allow")) {
							ruleType = RuleType.ALLOW_ALL;
						} else if (typeString.equals("disallow")) {
							ruleType = RuleType.DISALLOW_ALL;
						} else {
							// this shouldn't happen
							throw new RuntimeException("Invalid rule type "
									+ typeString);
						}
					} else {
						scope = capitalizePercentEncoding(scope);
						if (typeString.equals("user-agent")) {
							// lower-case agents just to be sure
							ruleType = RuleType.USER_AGENT;
							scope = scope.toLowerCase().trim();
						} else if (typeString.equals("allow")) {
							ruleType = RuleType.ALLOW;
						} else if (typeString.equals("disallow")) {
							ruleType = RuleType.DISALLOW;
						} else if (typeString.equals("crawl-delay")) {
							ruleType = RuleType.CRAWL_DELAY;
						} else if (typeString.equals("sitemap")) {
							ruleType = RuleType.SITEMAP;
						} else {
							// this shouldn't happen
							throw new RuntimeException("Invalid rule type "
									+ typeString);
						}
					}
					rules.add(new Rule(ruleType, scope));
				} else {
					if (!line.matches("\\s*#.*")) {
						LOG.info("Encountered a bad line in robot file: "
								+ con.getURL() + "    " + line);
					}
				}
			}
			this.rules = Collections.unmodifiableList(rules);
		} catch (UnsupportedEncodingException e) {
			throw new PageCrawlException(e);
		} catch (IOException e) {
			throw new PageCrawlException(e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					throw new PageCrawlException(e);
				}
			}
		}
	}

	public RobotsTXT(String agent) {
		rules = Collections.emptyList();
		expTime = 0;
		this.agent = agent;
	}

	private String capitalizePercentEncoding(String scope) {
		Matcher scopeMatcher = scopePattern.matcher(scope);
		StringBuffer goodScope = new StringBuffer();
		// change the % encoding strings in the scope
		while (scopeMatcher.find()) {
			String encodedPercent = scopeMatcher.group();
			scopeMatcher.appendReplacement(goodScope,
					encodedPercent.toUpperCase());
		}
		scopeMatcher.appendTail(goodScope);
		return goodScope.toString();
	}

	@SuppressWarnings("incomplete-switch")
	public boolean allowed(String url) {
		url = capitalizePercentEncoding(url);
		boolean rulesApply = true;
		boolean urlAllowed = true;
		for (Rule rule : rules) {
			switch (rule.type) {
			case USER_AGENT_ALL:
				rulesApply = true;
				break;
			case USER_AGENT:
				rulesApply = rule.scope.equals(agent);
				break;
			case ALLOW:
				if (rulesApply) {
					urlAllowed = urlAllowed || url.startsWith(rule.scope);
				}
				break;
			case ALLOW_ALL:
				if (rulesApply) {
					urlAllowed = true;
				}
				break;
			case DISALLOW:
				if (rulesApply) {
					urlAllowed = urlAllowed && !url.startsWith(rule.scope);
				}
				break;
			case DISALLOW_ALL:
				if (rulesApply) {
					urlAllowed = false;
				}
				break;
			}
		}
		return urlAllowed;
	}

	@SuppressWarnings("incomplete-switch")
	public long getDelay() {
		boolean rulesApply = true;
		long crawlDelay = 0;
		for (Rule rule : rules) {
			switch (rule.type) {
			case USER_AGENT_ALL:
				rulesApply = true;
				break;
			case USER_AGENT:
				rulesApply = rule.scope.equals(agent);
				break;
			case CRAWL_DELAY:
				if (rulesApply)
					crawlDelay = Long.parseLong(rule.scope) * 1000;
			}

		}
		return crawlDelay;
	}

	public static RobotsTXT get(URL url, String agent)
			throws PageCrawlException {
		agent = agent.toLowerCase().trim();
		int port = url.getPort();
		if (port == -1)
			port = url.getDefaultPort();
		String robotsLoc = url.getProtocol() + "://" + url.getAuthority() + "/robots.txt";
		UrlAndAgent urlAndAgent = new UrlAndAgent(robotsLoc, agent);
		RobotsTXT robots = CACHE.get(urlAndAgent);
		if (robots == null || robots.expTime > System.currentTimeMillis()) {
			// we use a connection here because it allows easier
			// detection of 404s.
			URLConnection con;
			try {
				con = new URL(robotsLoc).openConnection();
				con.connect();
			} catch (IOException e) {
				throw new PageCrawlException(e);
			}
			try {
				if (con instanceof HttpURLConnection
						&& ((HttpURLConnection) con).getResponseCode() > 400) {
					// if there's an error we assume it doesn't exist
					robots = new RobotsTXT(agent);
				} else {
					long exp = con.getExpiration();
					if (exp == 0) {
						exp = 1000 * 60 * 60; // 1hr
					}
					robots = new RobotsTXT(con, agent, exp);
				}
			} catch (IOException e) {
				throw new PageCrawlException(e);
			}
			CACHE.put(urlAndAgent, robots);
		}
		return robots;
	}
}
