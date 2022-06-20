import requests
import socket
import time
import os
import random as rand
from bs4 import BeautifulSoup as soup, Tag
import json
from tgFetch import tgFetch
from util_funcs import *


DATA_PATH = "../data/"
PATH_TO_DATA = f"{DATA_PATH}information.json"

class tgScraper(tgFetch):
    opts = ['tgme_widget_message_text','tgme_widget_message_photo_wrap','tgme_widget_message_video_wrap']
   
    def __init__(self, URL):
        super().__init__(URL)
        
    def __init__(self, URL, HOST, PORT, translator):
        super().__init__(URL, HOST, PORT, translator)

    def loadSample(filename):
        f = open(filename, "r", encoding="utf-8")
        return f.read()

    def saveToFile(file,content):
        with open(file, "w", encoding="utf-8") as f:
            f.write(content)
        return True

    def dictToFile(file,dict,type='w'):
        try:
            with open(file, type, encoding="utf-8") as outfile:
                    json.dump(dict, outfile, ensure_ascii=False)
                    return True
        except Exception as e: print(f"(dictToFile): {e}")

    #carica la pagina, al momento prende come parametri "file" o "http"     
    def load(self, type = "http", address = None):
        if type == "file" and address != None:
            return tgScraper.loadSample(address)

        address = self.URL if address==None else address
        print(f"\nLoading {address} ...")
        response = requests.get(address)

        if response.ok:
            # print(response.json)
            return response.text
        else:
            print("[ERRORE]: la pagina {address} non è stata caricata")
            return -1


    #estrae l'ID
    def extractID(msg):
        data_post = msg.find('div',{'class':'tgme_widget_message'})['data-post']
        ID = data_post[data_post.find('/')+1:]
        return ID

    def getAuthorName(msg):
        return msg.find('div',{'class':'tgme_widget_message_author'}).span.text

    def getAuthorLink(msg):
        return msg.find('div',{'class':'tgme_widget_message_author'}).a["href"]

    def getMessageDate(msg):
        return msg.find('a',{'class':'tgme_widget_message_date'}).time["datetime"]

    def getMessageViews(msg):
        return msg.find('span',{'class':'tgme_widget_message_views'}).text

    #estrae un vettore contenente il testo presente in un messaggio
    def extract_text(str):
        str = str.find('div',{'class':'tgme_widget_message_text'})
        text=[]
        for elem in str.contents:
            if isinstance(elem, Tag):
                elem = elem.text
            text.append(elem)
        return list(filter(lambda s: not(s=="" or s==" "),text))

    #ottiene la lista contenente i link alle immagini
    def findImgsLink(str):
        imgs = str.findAll(['div','a'],{'class':'tgme_widget_message_photo_wrap'})
        imgLinks = []
        for img in imgs:
            img = img['style']
            _link = img[img.find("(")+2:img.find("')")]
            imgLinks.append(_link)
        return imgLinks
    
    #ottiene la lista contenente i link ai video
    def findVideos(str):
        vids = []

        for vid in str.findAll('a',{'class':'tgme_widget_message_video_player'}):
            vids.append(vid.get('href'))
            
        return vids

    #ottiene una lista di liste con tutte le classi che potrebbero rappresentare dati importanti
    def getClassCandidates(str):
        takeClass = lambda x: x['class']
        candidates = str.find('div',{'class':'tgme_widget_message_bubble'}).findAll(['div','a'])
        return list(map(takeClass, candidates))

    #ritorna una lista con tutte le classi rilevanti
    def getRelevantClasses(message, relevant = opts):
        # print("************************")
        tags = message.find('div',{'class':'tgme_widget_message_bubble'}).findAll(['div','a'])
        # print(relevant)

        # print(f"num tags: {len(tags)}")

        classes = list(map(lambda x: list(filter(lambda y: y in relevant, x['class'] if x.has_attr("class") else [])), tags))
        # print(f"classes: {classes}")
        useful_classes = list(filter(lambda x: len(x)>0, classes))
        # print(f"useful_classes: {useful_classes}")
        class_list = [item for sublist in useful_classes for item in sublist]
        # print(class_list)

        # print("************************")
        return class_list

    #ritorna una lista con elementi distinti
    def getDistinct(arr):
        return list(set(arr))
    
    #crea un dizionario dove per ogni classe html rilevante si associa la relativa funzione
    #che permette di estrapolare quello specifico dato
    def bindClassesToFuncs():
        classFunc = {}
        get_text = lambda x : tgScraper.extract_text(x)
        get_images = lambda x : tgScraper.findImgsLink(x) 
        get_videos = lambda x : tgScraper.findVideos(x)
        #il seguente dizionario verrà popolato dinamicamente da un file di configurazione
        #contenente le associazioni classe html/metodo da invocare per ottenere quel dato
        classFunc["tgme_widget_message_text"] = ["text",get_text]
        classFunc["tgme_widget_message_photo_wrap"] = ["images",get_images]
        classFunc["tgme_widget_message_video_wrap"] = ["videos",get_videos]
        return classFunc

    #traduce il testo di un messaggio
    def getTranslation(text, translator):
        if isinstance(text, str):
            return translator.randomTranslate(text)
        if isinstance(text, list):
            if len(text) == 0:
                return []
            #se l'array contiene almeno una stringa le unisco separandole da un carattere di ritorno a capo
            jointText = '\n'.join(text)
            translated = translator.randomTranslate(jointText)
            translated['text'] = translated['text'].split('\n')
            print(f"traduction: {translated}")
            return translated 
        return None
    
    #ottiene un dizionario contenenti i dati del messaggio
    def dictFromMessage(msg, channel="",translator = None):
        dict = {}

        #aggiunge il nome del canale
        if channel != "":
            dict["channel"] = channel

        #aggiunge ID messaggio
        dict["id"] = tgScraper.extractID(msg)

        #aggiunge info sull'autore del messaggio
        dict["authorName"] = tgScraper.getAuthorName(msg)
        dict["authorLink"] = tgScraper.getAuthorLink(msg)

        #aggiunge info quali testo, link a video e immagini
        classFunc = tgScraper.bindClassesToFuncs()
        relevant = tgScraper.getDistinct(tgScraper.getRelevantClasses(msg))
        for c in relevant:
            dict[classFunc[c][0]] = classFunc[c][1](msg)

        #aggiunge traduzione, se richiesto
        if translator != None:
            try:
                dict["translation"] = tgScraper.getTranslation(dict["text"],translator)
            except Exception as e: print(f"(Translation): {e}")

        #aggiunge altre info
        try:
            dict["views"] = tgScraper.getMessageViews(msg)
        except Exception as e: print(f"(Message Views): {e}")
        try:
            dict["data"] = tgScraper.getMessageDate(msg)
        except Exception as e: print(f"(Message Date): {e}")

        return dict

    #controlla se il messaggio ha un id, ritorna True o False
    def hasID(container):
        try:
            data_post = container.find('div',{'class':'tgme_widget_message'})['data-post']
            if int(data_post[data_post.find('/')+1:]) >= 0:
                return True
            else:
                print("L'id trovato non è un intero positivo")
                return False
        except:
            print("Trovato un post senza ID[SCARTATO]")
            return False

    #crea un nested dictionary con un dictionary per ogni messaggio(container)
    def containers2dicts(containers, name="", min_id=1,translator=None):
        dicts = []               
        for container in containers:
            if tgScraper.hasID(container) and int(tgScraper.extractID(container)) >= min_id:
                dicts.append(tgScraper.dictFromMessage(container,channel=name,translator=translator))
        return dicts

    def sendTo(self,dict,toFile=None, stdout=False, sendTCP=False):
        #se il dizionario(o la lista) è vuoto allora esce
        if dict == {} or dict == []:
            print("DICT E' VUOTO")
            return None

        #stampa a video
        if stdout == True:
            print("get Last Messages:")
            json_object = json.dumps(dict, indent = 2, ensure_ascii=False) 
            print(json_object)

        #salva in un file
        if toFile!=None:
            print(f"Saving messages to {toFile}.")
            tgScraper.dictToFile(toFile,dict)
        
        #manda i dati tramite TCP
        if sendTCP == True:
            self.sendToTCP(dict) 

    def saveData(param, data):
        if not os.path.exists(PATH_TO_DATA):
            open(PATH_TO_DATA, "w")
            tgScraper.saveToFile(PATH_TO_DATA,"{}")
        f = open(PATH_TO_DATA)
        obj = {}
        try:
            obj = json.load(f)
            if not isinstance(obj, dict):
                obj = {}
        except:
            obj = {}
        obj[param] = data
        try:
            obj['transaction_id'] = (obj['transaction_id'] + 1) % 10000 
        except:
            obj['transaction_id'] = 1
        text = json.dumps(obj)
        tgScraper.saveToFile(PATH_TO_DATA,text)

    #ottiene gli ultimi messaggi
    def getLastMessages(self,toFile=None, stdout=False, sendTCP=False, batch=True, sleepTime = 25):
        last_id = 0
        self.response = ""
        dicts = []
        toFile_path = None if toFile == None else f"{DATA_PATH}{toFile}"
        while True:
            self.response = self.load()
            if self.response == -1:
                return -1
            # self.response = tgScraper.loadSample("containers.txt")
            # self.soup = soup(self.response,features="html.parser")
            # print(f"lingua di traduzione = {self.translator.TO}")
            new_dicts = tgScraper.html2dicts(self.response, self.chname, min_id= last_id + 1, translator = self.translator)
            last_id = max(tgScraper.getIdList(self.response))
            self.sendTo(dict=new_dicts,stdout=stdout,sendTCP=sendTCP)
            tgScraper.saveData("last_id",last_id)
            dicts += new_dicts
            
            self.sendTo(dict=dicts, toFile=toFile_path)
            if batch:
                break
            print("Waiting for new messages...")
            time.sleep(sleepTime)
        return dicts


    def loadParametricMessage(self, params):
        param_string = ""
        for p in params:
            param_string = f"{param_string}{p[0]}={p[1]}&"
        param_string = param_string[:-1]
        msg_url = f"https://t.me/s/{self.chname}?{param_string}"
        print(f"[loadParametricMessage] = {msg_url}")
        response = requests.get(msg_url)
        if response.ok:
            return response
        else:
            print("[ERRORE]: la pagina {msg_url} non è stata caricata")
            return -1

    def loadMessageByID(self, ID,params=[]):
        param_string = ""
        if params!=[]:
            param_string = "?"
            for p in params:
                param_string = f"{param_string}{p[0]}={p[1]}&"
            param_string = param_string[:-1]
        # print(chname)
        msg_url = f"https://t.me/s/{self.chname}/{ID}{param_string}"
        print(f"[loadMessageByID] = {msg_url}")
        response = requests.get(msg_url)
        if response.ok:
            return response
        else:
            print("[ERRORE]: la pagina {msg_url} non è stata caricata")
            return -1


    #type può essere o "http" o "file" o "string"
    def countMessagesInRequest(self, type = "http", address = None, saveTo = None, source = None):
        if type == "string":
            request = source
            address = ""
        else:
            if address != None:
                print(f"Addresss {address}") 
            request = self.load(type = type, address = address)
        msg_soup = soup(request,features="html.parser")
        containers = msg_soup.findAll('div',{'class':'tgme_widget_message_wrap'})
        num_containers = len(containers)
        if containers[0].text=="No posts found" and num_containers == 1:
            num_containers = 0
        print(f"Sono presenti {num_containers} messaggi nell'ultima richiesta {address}")
        if saveTo != None:
            tgScraper.saveToFile(saveTo,request)
        return num_containers

    def getIdList(request):
        msg_soup = soup(request,features="html.parser")
        containers = msg_soup.findAll('div',{'class':'tgme_widget_message_wrap'})
        ids = []    #lista degli id dei messaggi presenti
        for container in containers:
            if tgScraper.hasID(container):
                ids.append(int(tgScraper.extractID(container)))
        print(f"{ids}, max: {max(ids if ids != [] else [0])}")
        return ids
    
    #ritorna il numero di messaggi nella richiesta e la lista degli id presenti
    def getInfoAboutRequests(self, channel, times = 1):
        while True:
            randNum = rand.randint(1,100)
            address = f"https://t.me/s/{channel}?after={randNum}"
            request = self.load(type = "http", address = address)
            ids = tgScraper.getIdList(request)
            num_containers = len(ids)    #numero di messaggi
            print(f"Elaborazione richiesta:{address}\nids:{ids}\tnum:{num_containers}")
            time.sleep(5)
            times -= 1
            if times == 0 :
                break
        return True

    def findContainers(response):
        _soup = soup(response,features="html.parser")
        containers = _soup.findAll('div',{'class':'tgme_widget_message_wrap js-widget_message_wrap'})
        return containers

    def html2dicts(html,name, min_id=1, translator=None):
        containers = tgScraper.findContainers(html)
        dicts = tgScraper.containers2dicts(containers, name, min_id, translator)
        return dicts
    
    def sendToTCP(self, dict):
        data = json.dumps(dict, ensure_ascii=False)
        i = 0
        time_to_sleep = 5
        while True:
            try:
                # Create a socket (SOCK_STREAM sta per TCP socket)
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # Connect to server and send data
                print("Looking for a connection...")
                sock.connect((self.HOST, self.PORT))
                sock.sendall(bytes(data,encoding="utf-8"))
                print("[client] SENT")
                sock.close()
                break
            except:
                i += 1
                time.sleep(time_to_sleep) 
                time_to_sleep = min(15, time_to_sleep + rand.randint(1,i+1)/10)
                # print(f"Tentativo di connessione fallito [n.{i}]")
        return False

    def getMaxId(html):
        ids = tgScraper.getIdList(html)
        if ids == []:
            return 0
        return max(ids)

    def getAllMessages(self, min_id = None, max_id = None, query = None, sendTCP = False, toFile=None, stdout=False, batch=True, sleepTime = 25):
        print(f"Getting all messages from {self.URL}")
        parameters_list = [["q",query.replace(" ","+")]] if query != None else []
        current_id = min_id if min_id != None else 1
        html = self.loadMessageByID(current_id,parameters_list).text
        # tgScraper.saveToFile("file_speriamo.txt",html)
        dicts = tgScraper.html2dicts(html, self.chname,min_id = current_id+1, translator = self.translator)
        current_id = tgScraper.getMaxId(html)
        toFile_path = None if toFile == None else f"{DATA_PATH}{toFile}"
        self.sendTo(dict=dicts, toFile=toFile_path, stdout=stdout,sendTCP=sendTCP)
        print(f"\nCurrent id: {current_id}\n\n")
        tgScraper.saveData("last_id",current_id)

        parameters_list.append(["after",current_id])
        time_to_sleep = sleepTime
        time.sleep(sleepTime)
        while True:
            html = self.loadParametricMessage(parameters_list).text
            if self.countMessagesInRequest(type="string",source = html) > 0 and current_id < max(tgScraper.getIdList(html)): 
                current_id = tgScraper.getMaxId(html)
                parameters_list[len(parameters_list)-1][1] = current_id
                new_dicts = tgScraper.html2dicts(html, self.chname, translator = self.translator) 
                self.sendTo(dict=new_dicts, stdout=stdout,sendTCP=sendTCP)
                tgScraper.saveData("last_id",current_id)
                dicts += new_dicts
                self.sendTo(dict=dicts, toFile=toFile_path)
                time_to_sleep = sleepTime + rand.randint(1,40)/10
            else:
                if batch:
                    break
                print("Non ci sono nuovi messaggi...")
                time_to_sleep = min(time_to_sleep+rand.randint(1,25)/10, 60)
            print(f"\nCurrent id: {current_id}\n\n")
            if max_id != None and current_id >= max_id:
                print("Processo terminato")
                break
            print(f"Waiting for {time_to_sleep}s...")
            time.sleep(time_to_sleep)
        return dicts

 
    

    #TODO:
    #   Query parameter if not found [DONE]
    #   Hard limit in data fetch    [DONE]
