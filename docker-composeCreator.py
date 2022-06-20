import configparser
import docker
import subprocess
import sys
import json
import threading
import time

IMAGE_CLIENT_TCP = "client-tcp"
IMAGE_LOGSTASH = "logstash-for-ukraine"
IMAGE_ELASTICSEARCH = "elasticsearch:7.9.2"
IMAGE_KIBANA = "kibana:7.9.2"
IMAGE_ZOOKEEPER = "confluentinc/cp-zookeeper:7.0.1"
IMAGE_KAFKA = "confluentinc/cp-kafka:7.0.1"
IMAGE_KAFKAUI = "provectuslabs/kafka-ui"
IMAGE_KAFKAINIT = "confluentinc/cp-kafka:6.1.1"


def toBool(str):
    return False if str.lower() == "false" else True


def conf2EnvFormat(config, section, prefix="", backslash=0):
    backslash_str = "    " * backslash
    str = ""
    for item in config[section].items():
        option, param = item
        str += f"{backslash_str}- {prefix}{option.upper()}={param}\n"
        # print(f"1){item}")
    return str


def createScriptContainer(config, channel_name, port):
    service_name = channel_name.lower()
    container = f"""
            {service_name}:
                build:
                    context: clientTCP
                    dockerfile: Dockerfile  
                image: {IMAGE_CLIENT_TCP}
                # ports:
                #     - "{port}:{port}"
                networks:
                    - warplatforms-network
                volumes:
                    - $PWD/clientTCP/app/:/app/
                    - $PWD/clientTCP/data/{service_name}/:/data
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
        containers += createScriptContainer(config, chname, 5000 + i)
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
                image: {IMAGE_LOGSTASH}
                volumes:
                    - $PWD/LogstashDocker/pipeline/:/usr/share/logstash/pipeline/
                environment:
                    # limite RAM di 1gb.
                    - "LS_JAVA_OPTS=-Xms1g -Xmx1g"
                ports:
                    - "10155:10155"
                networks:
                    - warplatforms-network
                profiles: ["ingestion", "all"]
            
            zookeeper:
                image: {IMAGE_ZOOKEEPER}
                # container_name: zookeeper
                environment:
                    ZOOKEEPER_CLIENT_PORT: 2181
                    ZOOKEEPER_TICK_TIME: 2000
                networks:
                    - warplatforms-network
                
            kafkaserver:
                image: {IMAGE_KAFKA}
                # container_name: kafkaserver
                ports:
                    - "9092:9092"
                depends_on:
                    - zookeeper
                environment:
                    KAFKA_BROKER_ID: 1
                    KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
                    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
                    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafkaserver:29092
                    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
                    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
                    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
                networks:
                    - warplatforms-network
            
            kafka-ui:
                image: {IMAGE_KAFKAUI}
                # container_name: kafka-ui
                depends_on:
                    - zookeeper
                    - kafkaserver
                ports:
                    - "8080:8080"
                restart: always
                environment:
                    KAFKA_CLUSTERS_0_NAME: local
                    KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafkaserver:29092
                    KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181
                networks:
                    - warplatforms-network
            
            kafka-init:
                image: {IMAGE_KAFKAINIT}
                depends_on:
                    - kafkaserver
                    - zookeeper
                    - kafka-ui
                entrypoint: [ '/bin/sh', '-c' ]
                command: |
                    "
                    # blocks until kafka is reachable
                    kafka-topics --bootstrap-server kafkaserver:29092 --list
                    echo -e 'Creating kafka topics'
                    kafka-topics --bootstrap-server kafkaserver:29092 --create --if-not-exists --topic telegram-messages --replication-factor 1 --partitions 1
                    echo -e 'Successfully created the following topics:'
                    kafka-topics --bootstrap-server kafkaserver:29092 --list
                    "
                networks:
                    - warplatforms-network
                
            elasticsearch: 
                image: {IMAGE_ELASTICSEARCH} 
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
                    - warplatforms-network
                profiles: ["storage", "all"] 
                
            kibana: 
                image: {IMAGE_KIBANA} 
                ports: 
                    - '5601:5601' 
                networks: 
                    - warplatforms-network
                mem_limit: 1g
                profiles: ["visualization", "all"] 
                
            # prova_es8: 
            #     image: elasticsearch:8.2.2 
            #     ports: 
            #         - '9200:9200'
            #     environment: 
            #         - discovery.type=single-node
            #         - xpack.security.enabled=false
            #         - "ES_JAVA_OPTS=-Xms2g -Xmx2g" 
            #     mem_limit: 4g
            #     ulimits: 
            #         memlock: 
            #             soft: -1 
            #             hard: -1 
            #     networks: 
            #         - warplatforms-network
            #     profiles: ["storage"] 
             
            # prova_kibana8: 
            #     image: kibana:8.2.2 
            #     ports: 
            #         - '5601:5601' 
            #     networks: 
            #         - warplatforms-network
            #     mem_limit: 1g
            #     profiles: ["visualization"] 
                
            spark:
                build: 
                    context: spark
                networks: 
                    - warplatforms-network
                depends_on:
                    - elasticsearch
                    - kibana
                    - zookeeper
                    - kafkaserver
                profiles: ["computation", "all"] 

    networks:
        warplatforms-network:
            name: warplatforms-network
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
    print(
        f"Non è stata trovata alcuna configurazione corrispondente a {chname} ")

# TODO
# change channel conf method
# restart channel


def readChannels():
    global config
    return config.sections()


def insertOptions():
    newSection = {}
    while True:
        attr = input(
            "Inserisci un attributo (o 'done' per completare la creazione):")
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

    # aggiorna il config.ini
    with open('config.ini', 'w') as configfile:
        config.write(configfile)

    # modifica il docker-compose.yml
    saveDockerCompose()

    print("Ora i canali sono:")
    print(config.sections())
    # TO FINISH


def getContainersFromImage(image_name):
    global dockerClient
    containers_list = dockerClient.containers.list()
    return list(filter(lambda x: x.attrs['Config']['Image'] == image_name, containers_list))


def extractEnvironmentVariable(container, var_name):
    container.reload()
    substr = f"{var_name}="
    for env in container.attrs['Config']['Env']:
        if var_name in env:
            return env[len(substr):]
    return False


def printLastIds():
    for container in getContainersFromImage(IMAGE_CLIENT_TCP):
        print(
            f"{container.attrs['Config']['Labels']['com.docker.compose.service']} LAST_ID: {extractEnvironmentVariable(container,'ENV_LAST_ID')}")


def updateLastIds():
    global config
    modified = False
    for section in config.sections():
        if toBool(config[section]["overwrite_last_id"]):
            obj = {}
            try:
                obj = json.load(
                    open(f"clientTCP/data/{section.lower()}/information.json"))
                # transaction_id permette di capire se il dato è stato modificato o no rispetto all'ultimo aggiornamento
                if config[section]['transaction_id'] != str(obj['transaction_id']):
                    # print(f"[{section}]--> {config[section]['last_id']} to {obj['last_id']}")
                    config[section]['last_id'] = str(obj['last_id'])
                    config[section]['transaction_id'] = str(
                        obj['transaction_id'])
                    modified = True
            except Exception as e:
                # print(f"(updateLastIds[{section}]): {e}")
                continue
    if modified:
        # aggiorna il config.ini
        with open('config.ini', 'w') as configfile:
            config.write(configfile)
        # modifica il docker-compose.yml
        saveDockerCompose()


def containerRestart(params):
    if params[0][0] == '-':
        if params[0] == "-f" or params[0] == "-force" or params[0] == "--force":
            subprocess.run(["docker-compose", "kill"] +
                           list(map(lambda t: t.lower(), params[1:])))
        else:
            subprocess.run(["docker-compose", "stop"] +
                           list(map(lambda t: t.lower(), params[1:])))
        subprocess.run(["docker-compose", "up"] +
                       list(map(lambda t: t.lower(), params[1:]))+["--detach"])
    else:
        subprocess.run(["docker-compose", "stop"] +
                       list(map(lambda t: t.lower(), params)))
        subprocess.run(["docker-compose", "up"] +
                       list(map(lambda t: t.lower(), params))+["--detach"])


commands_info = {
    " - start": "Esegue i container previsti nel docker compose in modalità detatch.",
    " - up": "Esegue i container previsti nel docker compose in modalità detatch. Rimuove i container orfani",
    " - list": "Mostra la lista dei container in esecuzione e interrotti",
    " - shell": "Esegue un normale comando su shell \n\t(shell <comando>)",
    " - stop": "Permette di stoppare uno o più container \n\t(stop [<nome_servizio>])",
    " - run-profile": "Permette di eseguire soltanto i container del docker-compose relativi ad uno specifico profilo \n\t(run-profile <nome_profilo>)",
    " - channels": "Stampa i canali gestiti",
    " - conf":  "Visualizza i parametri di configurazione di uno specifico canale \n\t(conf <nome_canale>)",
    " - add": "Aggiunge un nuovo canale \n\t(add <nome_canale>)",
    " - run": "Esegui i container passati in input  \n\t(run [<nome_canale>])",
    " - update": "carica la versione corrente del config.ini, utile in caso di modifiche effettuate direttamente sul file",
    " - exit": "Termina l'esecuzione",
    " - restart": "Stoppa(o killa) i container passati in input \n\t(restart       [<nome_canale>]) STOP AND RUN \n\t(restart --force [<nome_canale>]) KILL AND RUN",
    " - kill": "Stoppa immediatamente i container passati in input",
}


def printCommandsInfo():
    for command in commands_info:
        print(f"{command}: {commands_info[command]}\n")
    return 0


commands = {
    "shell": lambda x: subprocess.run(x),
    "up": lambda x: subprocess.run(["docker-compose", "--profile", "all", "up", "--detach"]),
    "start": lambda x: subprocess.run(["docker-compose", "--profile", "fetching", "up", "--detach"]),
    "run-profile": lambda x: subprocess.run(["docker-compose", "--profile", x[0], "up", "--detach"]),
    "run": lambda x: subprocess.run(["docker-compose", "up"]+list(map(lambda t: t.lower(), x))+["--detach"]),
    "update": lambda x: updateConfig(),
    "list": lambda x: subprocess.run(["docker-compose", "ps"]),
    "channels": lambda x: print(readChannels()),
    "stop": lambda x: subprocess.run(["docker-compose", "stop"]+list(map(lambda t: t.lower(), x))),
    "kill": lambda x: subprocess.run(["docker-compose", "kill"]+list(map(lambda t: t.lower(), x))),
    "add": lambda x: addNewChannel(x),
    "conf": lambda x: getConfAbout(x[0]),
    "exit": lambda x: sys.exit(0),
    "restart": lambda x: containerRestart(x),
    "help": lambda x: printCommandsInfo(),
}


def execCommand(command):
    params = command.split()
    if params == []:
        return
    if params[0].lower() in commands:
        answer = commands[params[0].lower()](params[1:])
        # print(answer)
    else:
        print(f"\tIl comando {command} non è previsto")
    return


def commandsPipeline(sleepTime=5):
    while True:
        # updateConfig()
        updateLastIds()
        time.sleep(sleepTime)


if __name__ == '__main__':

    dockerClient = docker.from_env()
    # container = dockerClient.containers.get('34f1571bb1')

    config = configparser.ConfigParser()
    config.read("config.ini")
    saveDockerCompose()

    # Avviare il thread che effettua una serie di azioni e controlli in background(tra cui l'aggiornamento dei last_id)
    pipelineThread = threading.Thread(target=commandsPipeline, args=(4,))
    # se il thread è un deamon dovrebbe terminare quando il main thread termina
    pipelineThread.daemon = True
    pipelineThread.start()

    # print("Running Docker-Compose Interactive Shell...")
    while True:
        command = input("#>")
        execCommand(command)
