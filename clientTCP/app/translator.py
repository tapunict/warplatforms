import random as rand
from bisect import bisect
import warnings
import translators as ts



class translator:
    
    lang_map = ["auto", "en", "ru", "uk", "it", "de", "ch"] #lingue previste (su cui ho fatto delle prove)

    # FROM              lingua/e da cui tradurre
    # TO                lingua in cui tradurre
    # list              lista di dizionari contenenti, ciascuno, nome del servizio di traduzione e relativo voto
    # partial_sums      serve al metodo randomTranslate per la scelta randomica del servizio di traduzione
    

    def sumAllVotes(self):
        sum = 0
        votes = list(map(lambda x : x["vote"],self.list))
        for vote in votes:
            sum += vote
        return sum

    def partialVotesSum(self):
        partial_sum = list(map(lambda x : x["vote"],self.list))
        for i in range(1,len(partial_sum)):
            partial_sum[i] = partial_sum[i] + partial_sum[i-1]
        return partial_sum

    def getRequiredLanguages(*args):
        requiredLanguages = []
        for arg in args:
            if isinstance(arg, str):
                arg = [arg]
            for elem in arg:
                if elem in translator.lang_map:
                    requiredLanguages.append(elem)
                else:
                    warnings.warn(f"Warning, unexpected language: {elem} not in list")
        return requiredLanguages

    #memorizza in self.list un array di dizionari contenenti, per ogni traduttore, il nome e il relativo voto.
    #Più il voto di un servizio di traduzione è alto più è probabile che venga scelto nel randomTranslate()
    def __init__(self, file="", dict="", FROM = "auto", TO = "en" ):
        if file != "":
            with open(file, "r", encoding="utf-8") as f:
                lines = f.readlines()
                # self.list = [{}] * len(lines);
                #  requiredLanguages = ['ru','en','uk']
                self.FROM = FROM
                self.TO = TO
                requiredLanguages = translator.getRequiredLanguages(FROM, TO)
                self.list = []
                for line in lines:
                    accepted = True
                    service,vote,*langs = line.strip().split()
                    for lang in requiredLanguages:
                        if lang not in langs:
                            accepted = False
                            break
                    if accepted:
                        self.list.append({"service":service,"vote":int(vote)})
        
        #ottiene un array delle somme parziali dei voti
        self.partial_sums = self.partialVotesSum()


    def randomTranslate(self,text):
        while True:
            #sceglie un intero random tra 0 e la somma totale dei voti -1(che corrisponde all'ultima somma parziale -1)
            rand_val = rand.randint(0,self.partial_sums[len(self.partial_sums)-1]-1)
            #trova la posizione in cui il numero random verrebbe inserito se fosse aggiunto alla lista delle somme parziali
            # => la posizione trovata corrisponde all'indice del traduttore da usare
            pos = bisect(self.partial_sums, rand_val)
            #preleva dalla lista il nome del servizio
            service = self.list[pos]["service"]
            try:
                #chiama il metodo translate
                translation = translator.translate(text=text,translator=service,FROM=self.FROM,TO=self.TO)
                return {"service":service,"text":translation}
            except Exception as e: print(f"({service} translator): {e}")

        #Da sistemare "auto" per reverso, warning:  warnings.warn('Unsupported [from_language=auto] with [reverso]! Please specify it.')
    def allServicesTranslate(self,text):
        for elem in self.list:
                service = elem["service"]
                try:
                    translation = translator.translate(text=text,translator=service,FROM=self.FROM,TO=self.TO)
                    print({"service":service,"translation":translation})
                except:
                    print(f"Non funziona {service}")
        return False
    def translate(text, FROM="auto", TO="en", translator="google"):
        # es.   ts.google(text)
        return getattr(ts, translator)(text,from_language=FROM,to_language=TO)#,from_language=FROM,to_language=TO

    def translateHTML(html_text, translator="google", to_language='en'):
        return ts.translate_html(html_text, translator=getattr(ts, translator), to_language=to_language, n_jobs=-1)