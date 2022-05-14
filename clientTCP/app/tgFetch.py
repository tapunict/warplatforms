class tgFetch:

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
        print(self.URL)

    def __init__(self, URL):
        self.formatUrl(URL)
        self.HOST = "localhost"
        self.PORT = 5000

    def __init__(self, URL, HOST, PORT):
        self.formatUrl(URL)
        self.HOST = HOST
        self.PORT = PORT   

#scraping => getLastMessages(self)
#API =>getLastMessages(self,28)

    def getLastMessages(self, num=0):
        print("get Last Messages")
        return False

    def getAllMessages(self):
        return False

    def getMessagesByTs(self,ts):
        return False


