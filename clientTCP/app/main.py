from tgScraper import *

#TO-DO:
#implementare la sendTCP anche per la getLastMessages

if __name__ == '__main__': 

    # fe = tgFetch("t.me/ciao")
    fetcher = tgScraper("https://t.me/UkraineNowItalian","logstash",10155)
    # fetcher = tgScraper("https://t.me/UkraineNow","logstash",10155)
    # fetcher = tgScraper("t.me/InteressanteTelegramChannel","logstash",10155)
    
    #Prende gli ultimi messaggi
    # fetcher.getLastMessages(stdout=True, sendTCP=True, batch=False)
    # fetcher.getLastMessages(stdout=True, toFile="ultimi_messaggi.json")

    #Ritorna tutti i messaggi
    # fetcher.getAllMessages(sendTCP=True, batch = False)

    #Ottiene tutti i messaggi che contengono una specifica keyword
    # fetcher.getAllMessages(query="strade",toFile="ultimi_messaggi.json")
    # fetcher.getAllMessages(query="strade",sendTCP=True)
    # fetcher.getAllMessages(query="carri+armati",sendTCP=True)
    # fetcher.getAllMessages(query="ucraini",sendTCP=True)


    #Ottiene tutti i messaggi da un minimo ad un massimo
    fetcher.getAllMessages(min_id=127,max_id=150,sendTCP=True)
    # fetcher.getAllMessages(min_id=127,max_id=150,sendTCP=True)
    # fetcher.getAllMessages(min_id=11600, sendTCP=True)
    # fetcher.getAllMessages(max_id=572)

    #Prende uno specifico messaggio
    # fetcher.getMessageByID(17)


    # fetcher.countMessagesInRequest(type="http",address="https://t.me/s/UkraineNowItalian/22",saveTo="newFile.txt")
    # fetcher.getInfoAboutRequests(channel="UkraineNowItalian",times = 7)
    # fetcher.getLastMessages(stdout=True)
    # fetcher.getMessageByID(17)
    # tgScraper.getChannelNameFromUrl("https://t.me/s/UkraineNowItalian")
    # fetcher.getLastMessages(toFile="channel_messages2.json", stdout=True)
    # f = open("sampleContainer.txt", "r")
    # msg = f.read()
    # tgScraper.dictFromMessage(msg)