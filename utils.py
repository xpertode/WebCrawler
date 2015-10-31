import socket
from urlparse import urlparse,urljoin
import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

#Proxy Conf
PROXIES_LIST = ["http://127.0.0.1:8118"]
proxy_index = 0

s = requests.session()

url_cache = dict()

def get_contact_info(url):
	# parsed_uri = urlparse(url)
	# domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
	domain = url
	if domain in url_cache:
		logger.info("Cache Search for %s successfull!!",domain)
		contact_link = url_cache[domain]
	else:
		contact_link = getcontactlink(domain)
		url_cache[domain] = contact_link
	
	if contact_link is not None:
		logger.info("Successfully crawled contact for %s,impressum is %s",domain,contact_link)
		return contact_link
	else:
		logger.info("Unable to find contact for %s",domain)
		return None


def get_proxy():
	global proxy_index
	proxy = PROXIES_LIST[proxy_index]
	proxy_index = (proxy_index + 1) % len(PROXIES_LIST)
	logger.info("Using proxy %s",proxy)
	return proxy

def getcontactlink(domain):
	try:
		logger.info("Crawling %s",domain)
		http_proxy = get_proxy()
		s.proxies = { "http"  : http_proxy }
		r = s.get(domain)
		link = find_contact(r.text)
		if link is not None:
			return urljoin(domain,link)
		else:
			return "http://www.bildplagiat.de"
	except:
		logger.error("Url not reachable: %s",domain)
		return None

def find_contact(html):
	soup = BeautifulSoup(html, 'html.parser')
	all_links = soup.find_all('a')
	# logger.info("Length %s",len(all_links))
	for link in all_links:
		if "impressum" in str(link).lower():
			return link.get('href')
		elif "imprint" in str(link).lower():
			return link.get('href')
		elif "kontakt" in str(link).lower():
			return link.get('href')
		elif "contact" in str(link).lower():
			return link.get('href')
		elif "bildnachweis" in str(link).lower():
			return link.get('href')

def update_table(impressum,sql_id,table="googleimagefound",column="imprint"):
	sql = 'update %s set %s = \"%s\" where id = %s;'%(table,column,impressum,sql_id)
	return sql

