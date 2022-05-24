import sys
from tgScraper import *
from translator import translator




if __name__ == '__main__': 
    channel = "https://t.me/" + sys.argv[1]
    last_id = sys.argv[2]

    ## language
    # input languages
    # print(ts.google(wyw_text)) # default: from_language='auto', to_language='en'
    # output language_map
    # print(ts._google.language_map)

    #effettua una traduzione
    tr = translator(file="translators.txt",FROM = "it", TO= "en")

    # fe = tgFetch("t.me/ciao")
    fetcher = tgScraper(channel,"logstash",10155,tr)
    # fetcher = tgScraper("https://t.me/UkraineNow","logstash",10155,tr)
    # fetcher = tgScraper("t.me/InteressanteTelegramChannel","logstash",10155,tr)
    russian_text = """
    Я хочу научиться играть на гитаре и фортепиано.
    Я с детства хотел завести собаку, но родители мне не разрешали. Пока я был ребёнком, у меня жил хомяк Хома. Хома был очень маленький и пушистый.
     Его шерсть была средней длинны и коричневого цвета. Родители купили большую клетку для него, с двумя этажами. Я был очень рад, когда у меня появился маленький друг.
     Было очень весело смотреть как Хома бегает в колесе. Мне нравилось кормить его морковкой и орехами."""

    # print(tr.list)
    # print(translator.translate(text="Я хочу научиться играть на гитаре и фортепиано", FROM="ru", TO="en", translator="argos"))
    # print(translator.translate(text="Я хочу научиться играть на гитаре и фортепиано", FROM="ru", TO="en", translator="tencent"))
    # print(tr.randomTranslate(text="Ciao, proviamo questo servizio. Gli alberi sono in fiore"))
    # tr.allServicesTranslate(russian_text)
    #Prende gli ultimi messaggi
    fetcher.getLastMessages(stdout=True, batch=False)
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