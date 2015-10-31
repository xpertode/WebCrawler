import socket
from urlparse import urlparse,urljoin
import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

#Proxy Conf

def read_proxy_list(file):
	a = []
	with open(file) as f:
		a = f.readlines()
	a = [s.replace('\n', '') for s in a]
	a = ["http://" + w  for w in a]
	return a

PROXIES_LIST = read_proxy_list("proxy.txt")
# PROXIES_LIST = ["http://127.0.0.1:8118"]
proxy_index = 0

headers = {
    'Accept': "*/*",
    'User-Agent': "Mozilla/5.0"
}

s = requests.session()

url_cache = dict()

def get_contact_info(url):
	parsed_uri = urlparse(url)
	domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
	if domain in url_cache:
		contact_link = url_cache[domain]
	else:
		contact_link = getcontactlink(domain)
		if contact_link == "http://www.bildplagiat.de":
			contact_link = getcontactlink(url)
		url_cache[domain] = contact_link
	
	if contact_link is not None:
		return contact_link
	else:
		return None


def get_proxy():
	global proxy_index

	proxy = PROXIES_LIST[proxy_index]
	proxy_index = (proxy_index + 1) % len(PROXIES_LIST)
	return proxy

def getcontactlink(domain):
	try:
		logger.info("Crawling %s",domain)	
		http_proxy = get_proxy()
		s.proxies = { "http"  : http_proxy }
		r = s.get(domain,headers=headers)
		link = find_contact(r.text)
		if link is not None:
			return urljoin(domain,link)
		else:
			return "http://www.bildplagiat.de"
	except:
		logger.error("Url not reachable: %s",domain)
		return None

def find_contact(html):
	result = dict()
	soup = BeautifulSoup(html, 'html.parser')
	all_links = soup.find_all('a')
	for link in all_links:
		if "impressum" in str(link).lower():
			return link.get('href')
		elif "imprint" in str(link).lower():
			result["imprint"] = link.get('href')
		elif "kontakt" in str(link).lower():
			result["kontakt"] = link.get('href')
		elif "contact" in str(link).lower():
			result["contact"] =  link.get('href')
		elif "bildnachweis" in str(link).lower():
			result["bildnachweis"] = link.get('href')
	if "imprint" in result:
		return result["imprint"]
	elif "kontakt" in result:
		return result["kontakt"]
	elif "contact" in result:
		return result["contact"]
	elif "bildnachweis" in result:
		return result["bildnachweis"]


def update_table(impressum,sql_id,table="googleimagefound",column="imprint"):
	sql = 'update %s set %s = \"%s\" where id = %s;'%(table,column,impressum,sql_id)
	return sql

