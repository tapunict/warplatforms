import configparser
import subprocess
import sys
# import warnings

def createScriptContainer(channel_name, id, port):
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
                depends_on:
                    - logstash
                command: python -u main.py {channel_name} {id}
                profiles: ["py_{service_name}", "fetching", "all"]
                """
    return container

def createContainers(channels,ids):
    containers = ""
    for i in range(0, len(channels)):
        channel_name = str(channels[i])
        last_message_id = int(ids[i])
        containers += createScriptContainer(channel_name,last_message_id, 5000 + i)
    return containers

def createDockerCompose(channels, ids):
    script = f"""
    version: '3.7'

    services:
    {createContainers(channels, ids)}
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

    networks:
        logstash-network:
            name: logstash-network
            driver: bridge"""
    return script

def readChannels():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config.get('Channel', 'channel_name').split() 

def addNewContainer():
    config = configparser.ConfigParser()
    config.read("config.ini")
    print(f"]{config.get('Channel', 'channel_name')}[")
    #TO FINISH
commands_info = {
    "start": "Esegue i container previsti nel docker compose in modalità detatch. Identico al comando up",
    "up": "Esegue i container previsti nel docker compose in modalità detatch. Identico al comando start",
    "list" : "Mostra la lista dei container in esecuzione e interrotti",
    "shell" : "Esegue un normale comando su shell \n\t(shell <comando>)",
    "stop" : "Permette di stoppare uno o più container \n\t(stop [<nome_servizio>])",
    "run-profile" : "Permette di eseguire soltanto i container del docker-compose relativi ad uno specifico profilo \n\t(run-profile <nome_profilo>)",
    "channels" : "Stampa i canali gestiti",
}
def printCommandsInfo():
    for command in commands_info:
        print(f"{command}: {commands_info[command]}\n")
    return 0

commands = {
    "shell": lambda x : subprocess.run(x),
    "up": lambda x : subprocess.run(["docker-compose","--profile", "fetching", "up", "--detach"]),
    "start" : lambda x : subprocess.run(["docker-compose","--profile", "fetching", "up", "--detach"]),
    "run-profile": lambda x : subprocess.run(["docker-compose","--profile", x[0], "up", "--detach"]),
    "list" : lambda x : subprocess.run(["docker-compose","ps"]),
    "channels" : lambda x : print(readChannels()),
    "stop" : lambda x : subprocess.run(["docker-compose","stop"]+x),
    "add" : lambda x : addNewContainer(),
    "help" : lambda x : printCommandsInfo(),
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

    channels = config.get('Channel', 'channel_name').split()
    ids = config.get('Channel', 'last_message_id').split()

    docker_compose = createDockerCompose(channels, ids)
    # original_stdout = sys.stdout
    with open("docker-compose.yml", 'w') as f:
        f.write(docker_compose)
    
    print("Running Docker-Compose Interactive Shell...")  
    while True:
        command = input("#>") 
        execCommand(command)

#RUNNING ONLY ONE SERVICE
#   subprocess.run(["docker-compose", "up", "--detach","<service_name>"])

#RUNNING ONLY SERVICES OF A CERTAIN PROFILE
#   subprocess.run(["docker-compose","--profile", "all", "up", "--detach"])
