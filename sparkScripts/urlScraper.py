import requests
import time
from bs4 import BeautifulSoup
from bs4.element import Comment
import re

esempioInput = """add1 http://mit.edu.com abc
add2 https://unict.com . abc
add3 www.google.be. uvw
add4 https://www.google.it. 123
add5 www.website.gov.us test2
Hey bob on www.test.com. 
another test with ipv4 http://192.168.1.1/test.jpg. toto2
website with different port number www.test.com:8080/test.jpg not port 80
www.website.gov.uk/login.html
test with ipv4 (192.168.1.1/test.jpg).
search at lorenzo.tap.com/ukraine"""

regexURL=r"\b((?:https?://)?(?:(?:www\.)?(?:[\da-z\.-]+)\.(?:[a-z]{2,6})|(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))(?::[0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])?(?:/[\w\.-]*)*/?)\b"

#ritorna l'array degli URL
def findAllUrls(text):
    matches = re.findall(regexURL, text)
    return matches

def ensureProtocol(url,protocol="https"):
    # print(f"[ENSURE PROTOCOL] {url}")
    base = "http"
    if isinstance(url,str):
        if url[:len(base)] != base:
            url = f"{protocol}://{url}"
    if isinstance(url,list):
        for i in range(0,len(url)):
            if url[i][:len(base)] != base:
                url[i] = f"{protocol}://{url[i]}"
    return url




def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)  
    return u" ".join(t.strip() for t in visible_texts)
    # return [t.strip() for t in visible_texts]


def loadAndParse(address):
    print(f"[LOAD AND PARSE] {address}")
    print(f"\nLoading {address} ...")
    time.sleep(0.01)
    try:
        response = requests.get(address, verify=False, timeout=7)
        if not response.ok:
            print("[ERRORE]: la pagina {address} non è stata caricata")
            # return None
            return "null"
        return text_from_html(response.text)
    except Exception as e: 
        print(f"(loadAndParse): {e}")
        return "null"


# if __name__ == '__main__': findAllUrls(esempioInput)
if __name__ == '__main__': loadAndParse(input("#>"))