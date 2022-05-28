import configparser
import subprocess
import sys
# import warnings
# 
def conf2EnvFormat(config,section,prefix="",backslash=0):
    backslash_str = "    " * backslash 
    str = ""
    for item in config[section].items():
        option, param = item
        str += f"{backslash_str}- {prefix}{option.upper()}={param}\n"
        # print(f"1){item}")
    return str

def createScriptContainer(config,channel_name, port):
    service_name = channel_name.lower()
    container = f"""
            {service_name}:
                build:
                    context: clientTCP
                    dockerfile: Dockerfile  
                image: client-tcp
                ports:
                    - "{port}:{port}"
                networks:
                    - logstash-network
                volumes:
                    - $PWD/clientTCP/app/:/app/
                    - $PWD/clientTCP/channelConfigs
                environment:
{conf2EnvFormat(config,channel_name,prefix="ENV_",backslash=5)}
                command: python -u main.py {channel_name}
                profiles: ["fetching", "all"]
                """
    return container

def createContainers(config):
    # for chname in config.sections():
    #     for item in config[chname].items():
    #         print(item)
    containers = ""
    i = 0
    for chname in config.sections():
        containers += createScriptContainer(config,chname, 5000 + i)
        i += 1
    return containers

def createDockerCompose(config):
    script = f"""
    version: '3.7'

    services:
    {createContainers(config)}
            logstash:
                build:
                    context: LogstashDocker
                    dockerfile: Dockerfile
                image: logstash-for-ukraine
                volumes:
                    - $PWD/LogstashDocker/pipeline/:/usr/share/logstash/pipeline/
                environment:
                    # limite RAM di 1gb.
                    - "LS_JAVA_OPTS=-Xms1g -Xmx1g"
                ports:
                - "10155:10155"
                networks:
                    - logstash-network
                profiles: ["ingestion", "all"]
                
            elasticsearch: 
                image: elasticsearch:7.9.2 
                ports: 
                    - '9200:9200' 
                environment: 
                    - discovery.type=single-node
                    - "ES_JAVA_OPTS=-Xms2g -Xmx2g" 
                mem_limit: 4g
                ulimits: 
                    memlock: 
                        soft: -1 
                        hard: -1 
                networks: 
                    - logstash-network
                profiles: ["storage", "all"] 
             
            kibana: 
                image: kibana:7.9.2 
                ports: 
                    - '5601:5601' 
                networks: 
                    - logstash-network
                mem_limit: 1g
                profiles: ["visualization", "all"] 

    networks:
        logstash-network:
            name: logstash-network
            driver: bridge"""
    return script


def saveDockerCompose():
    global config
    docker_compose = createDockerCompose(config)
    with open("docker-compose.yml", 'w') as f:
        f.write(docker_compose)

def updateConfig():
    global config
    config.read("config.ini")
    saveDockerCompose()

def getConfAbout(chname):
    global config
    print(f"Channel: {chname}\nConfiguration options:")
    for section in config.sections():
        if section.lower() == chname.lower():
            for item in config[section].items():
                option, param = item
                print(f"\t{option} : {param}")
            return True
    print(f"Non è stata trovata alcuna configurazione corrispondente a {chname} ")

#TODO
# change channel conf method
# restart channel

def readChannels():
    global config
    return config.sections()

def insertOptions():
    newSection = {}
    while True:
        attr = input("Inserisci un attributo (o 'done' per completare la creazione):")
        if attr == 'done':
            return newSection
        value = input(f"Inserisci il valore da assegnare a {attr}:")
        newSection[attr] = value
    
def addNewChannel(params):
    print("Configurazione di un nuovo canale")
    global config
    # print(config.sections())
    if params == []:
        chname = input("Nome del canale: ")
    else:
        chname = params[0]
    config[chname] = insertOptions()

    #aggiorna il config.ini
    with open('config.ini', 'w') as configfile:
       config.write(configfile)

    #modifica il docker-compose.yml
    saveDockerCompose()
    
    print("Ora i canali sono:")
    print(config.sections())
    #TO FINISH

commands_info = {
    "start": "Esegue i container previsti nel docker compose in modalità detatch. Identico al comando up",
    "up": "Esegue i container previsti nel docker compose in modalità detatch. Identico al comando start",
    "list" : "Mostra la lista dei container in esecuzione e interrotti",
    "shell" : "Esegue un normale comando su shell \n\t(shell <comando>)",
    "stop" : "Permette di stoppare uno o più container \n\t(stop [<nome_servizio>])",
    "run-profile" : "Permette di eseguire soltanto i container del docker-compose relativi ad uno specifico profilo \n\t(run-profile <nome_profilo>)",
    "channels" : "Stampa i canali gestiti",
    "conf" :  "Visualizza i parametri di configurazione di uno specifico canale \n\t(conf <nome_canale>)",
    "add" : "Aggiunge un nuovo canale \n\t(add <nome_canale>)",
    "run" : "Esegui i container passati in input  \n\t(run [<nome_canale>])",
    "update" : "carica la versione corrente del config.ini, utile in caso di modifiche effettuate direttamente sul file",
}

def printCommandsInfo():
    for command in commands_info:
        print(f"{command}: {commands_info[command]}\n")
    return 0

commands = {
    "shell": lambda x : subprocess.run(x),
    "up": lambda x : subprocess.run(["docker-compose","--profile", "all", "up", "--detach"]),
    "start" : lambda x : subprocess.run(["docker-compose","--profile", "fetching", "up", "--detach"]),
    "run-profile": lambda x : subprocess.run(["docker-compose","--profile", x[0], "up", "--detach"]),
    "run" : lambda x : subprocess.run(["docker-compose", "up"]+x+["--detach"]),
    "update" : lambda x : updateConfig(),
    "list" : lambda x : subprocess.run(["docker-compose","ps"]),
    "channels" : lambda x : print(readChannels()),
    "stop" : lambda x : subprocess.run(["docker-compose","stop"]+x),
    "add" : lambda x : addNewChannel(x),
    "conf" : lambda x : getConfAbout(x[0]),
    "help" : lambda x : printCommandsInfo(),
    "exit" : lambda x : sys.exit(0),
}

def execCommand(command):
    params = command.split()
    if params[0].lower() in commands:
        answer = commands[params[0].lower()](params[1:])
        print(answer)
    else:
        print(f"\tIl comando {command} non è previsto")  
    return    


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("config.ini")
    saveDockerCompose()
    
    print("Running Docker-Compose Interactive Shell...")  
    while True:
        command = input("#>") 
        execCommand(command)

#RUNNING ONLY ONE SERVICE
#   subprocess.run(["docker-compose", "up", "--detach","<service_name>"])

#RUNNING ONLY SERVICES OF A CERTAIN PROFILE
#   subprocess.run(["docker-compose","--profile", "all", "up", "--detach"])
