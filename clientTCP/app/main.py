import os
import sys
from time import sleep
from tgScraper import *
from translator import translator
from util_funcs import *



if __name__ == '__main__': 

    # Il canale viene passato come parametro all'esecuzione di main.py
    channel = "https://t.me/" + sys.argv[1]

    # I restanti parametri vengono passati come varabiali d'ambiente
    last_id = int(os.environ['ENV_LAST_ID'])
    overwrite_last_id = toBool(os.environ['ENV_OVERWRITE_LAST_ID'])
    method = os.environ['ENV_METHOD']
    batch = toBool(os.environ['ENV_BATCH'])
    toFile = strOrNone(os.environ['ENV_TO_FILE'])
    stdout = toBool(os.environ['ENV_STDOUT'])
    sendTCP = toBool(os.environ['ENV_SENDTCP'])
    translation = toBool(os.environ['ENV_TRANSLATION'])
    query = strOrNone(os.environ['ENV_QUERY'])
    translateFROM = os.environ['ENV_TRANSLATE_FROM'].split()
    translateTO = os.environ['ENV_TRANSLATE_TO']
    delay = int(os.environ['ENV_DELAY'])


    # print(last_id)
    # print(overwrite_last_id)
    # print(method)
    # print(type(batch))
    # print(f"toFile = {toFile}")
    # print(stdout)
    # print(sendTCP)
    # print(translation)
    # print(translateFROM)
    # print(translateTO)

    if toFile != None:
        toFile=f"{toFile}.json"

    #effettua una traduzione
    tr = None
    if translation == True:
        tr = translator(file="translators.txt",FROM = translateFROM, TO= translateTO)
    
    fetcher = tgScraper(channel,"logstash",10155,tr)
    # fetcher = tgScraper("https://t.me/UkraineNow","logstash",10155,tr)
    # fetcher = tgScraper("t.me/InteressanteTelegramChannel","logstash",10155,tr)
    
    if method.lower() == "all":
        fetcher.getAllMessages( min_id = last_id, max_id = None, query = query, sendTCP = sendTCP, toFile=toFile, stdout=stdout, batch=batch, sleepTime = delay)
    elif method.lower() == "new":
        fetcher.getLastMessages(toFile=toFile, stdout=stdout, sendTCP=sendTCP, batch=batch, sleepTime = delay)
    else:
        print(f"method: {method} isn't correct")

    # print(tr.list)
    # print(translator.translate(text="Я хочу научиться играть на гитаре и фортепиано", FROM="ru", TO="en", translator="argos"))
    # print(translator.translate(text="Я хочу научиться играть на гитаре и фортепиано", FROM="ru", TO="en", translator="tencent"))
    # print(tr.randomTranslate(text="Ciao, proviamo questo servizio. Gli alberi sono in fiore"))
    # tr.allServicesTranslate(russian_text)
    #Prende gli ultimi messaggi
    # fetcher.getLastMessages(stdout=True, batch=False)
    # fetcher.getLastMessages(stdout=True, sendTCP=True, batch=False)

    #THIS
    # fetcher.getLastMessages(sendTCP=True, batch=False)
    # fetcher.getLastMessages(stdout=True, toFile="ultimi_messaggi.json")

    #Ritorna tutti i messaggi
    # fetcher.getAllMessages(sendTCP=True, batch = False)

    #Ottiene tutti i messaggi che contengono una specifica keyword
    # fetcher.getAllMessages(query="strade",toFile="ultimi_messaggi.json")
    # fetcher.getAllMessages(query="strade",sendTCP=True)
    # fetcher.getAllMessages(query="carri+armati",sendTCP=True)
    # fetcher.getAllMessages(query="ucraini",sendTCP=True)


    #Ottiene tutti i messaggi da un minimo ad un massimo
    # fetcher.getAllMessages(min_id=last_id,max_id=150,sendTCP=True)
    # fetcher.getAllMessages(min_id=last_id,max_id=250,sendTCP=True)
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