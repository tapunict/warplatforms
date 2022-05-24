import configparser
import subprocess
import sys


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
                """
    return container


def createDockerCompose(channels, ids):
    containers = ""
    for i in range(0, len(channels)):
        channel_name = str(channels[i])
        last_message_id = int(ids[i])
        containers += createScriptContainer(channel_name,
                                            last_message_id, 5000 + i)
    return containers


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("config.ini")

    channels = config.get('Channel', 'channel_name').split()
    ids = config.get('Channel', 'last_message_id').split()

    docker_compose = f"""
    version: '3.7'

    services:
    {createDockerCompose(channels, ids)}
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
    original_stdout = sys.stdout
    with open("docker-compose.yml", 'w') as f:
        sys.stdout = f
        print(docker_compose)
        sys.stdout = original_stdout
    subprocess.run(["docker-compose", "--profile", "all", "up"])
