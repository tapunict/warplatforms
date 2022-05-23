from translator import translator

class tgFetch:

    # URL               URL del canale telegram
    # chname            Nome del canale telegram
    # HOST              Host a cui inviare i dati dei messaggi 
    # PORT              Porta a cui inviare i dati dei messaggi
    # translator        Oggetto traduttore, istanza della classe translator 

    def formatUrl(self, URL):
        self.URL = URL
        if URL.find("t.me/") == -1:
            print("Non è un link telegram")
            return -1
        if URL.find("t.me/s/") == -1:
            self.URL = self.URL.replace(".me/",".me/s/")
            print(f"Ricevuto: {URL}\t convertito in: {self.URL}")
        if URL.find("://t.me") == -1:
            self.URL = self.URL.replace("t.me","https://t.me")
            print(f"Ricevuto: {URL}\t convertito in: {self.URL}")
        self.chname = tgFetch.getChannelNameFromUrl(self.URL)

    def getChannelNameFromUrl(url):
        prev = "t.me/s/"
        chname = url[url.find(prev)+len(prev):]
        # print(chname)
        return chname

    def __init__(self, URL):
        self.formatUrl(URL)
        self.HOST = "localhost"
        self.PORT = 5000
        self.translator = None             #non effettua alcuna traduzione del testo

    def __init__(self, URL, HOST, PORT, translator = None):
        self.formatUrl(URL)
        self.HOST = HOST
        self.PORT = PORT
        self.translator = translator        #   istanza del traduttore                    


    def getLastMessages(self, num=0):
        print("get Last Messages")
        return False

    def getAllMessages(self):
        return False

    def getMessagesByTs(self,ts):
        return False


