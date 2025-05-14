import subprocess
import json
import os
import time
import argparse

def run_command(command, Text = True):
    result = subprocess.run(command, shell=True, text=Text, capture_output=True)
    if result.returncode != 0:
        print(f"Command failed: {command}\n{result.stderr}",flush=True)
        os._exit(1)
    return result

def run_command_no_raise(command, Text = True):
    result = subprocess.run(command, shell=True, text=Text, capture_output=True)
    if result.returncode != 0:
        print(f"Command failed: {command}\n{result.stderr}",flush=True)
    return result

def main():

    parser = argparse.ArgumentParser(description="Script to run commands with specified configurations.")
    parser.add_argument("--config-file", type=str, required=True, help="Path to the config file.")
    parser.add_argument("--new-image", type=str, help="New image version.")
    parser.add_argument("--old-image", type=str, help="Old image version.")
    parser.add_argument("--latest-image", type=str, help="Latest image version.")
    parser.add_argument("--only-restart", action="store_true", help="Flag to indicate if only restart is needed.")
    parser.add_argument("--build", action="store_true", help="Flag to indicate if need to rebuild image.")
    parser.add_argument("--app-port", type=str, required=True, help="Application port.")
    parser.add_argument("--service-name", type=str, required=True, help="Service name.")
    parser.add_argument("--project-name", type=str, required=True, help="Project name.")
    parser.add_argument("--nginx-container-name", type=str, required=True, help="NGINX container name.")
    parser.add_argument("--local-network", type=str, required=True, help="Local network.")

    args = parser.parse_args()

    CONFIG_FILE = args.config_file
    NEW_IMAGE = args.new_image
    OLD_IMAGE = args.old_image
    LATEST_IMAGE = args.latest_image
    isOnlyRestart = args.only_restart
    build = args.build
    APP_PORT = args.app_port
    SERVICE_NAME = args.service_name
    PROJECT_NAME = args.project_name
    NGINX_CONTAINER_NAME = args.nginx_container_name
    LOCAL_NETWORK = args.local_network

    if isOnlyRestart is False and (not NEW_IMAGE or not LATEST_IMAGE):
        print("Не переданы IMAGE аргументы при обновлении",flush=True)
        os._exit(1)

    print(f"CONFIG_FILE: {CONFIG_FILE}",flush=True)
    print(f"NEW_IMAGE: {NEW_IMAGE}",flush=True)
    print(f"OLD_IMAGE: {OLD_IMAGE}",flush=True)
    print(f"LATEST_IMAGE: {LATEST_IMAGE}",flush=True)
    print(f"isOnlyRestart: {isOnlyRestart}",flush=True)
    print(f"APP_PORT: {APP_PORT}",flush=True)
    print(f"SERVICE_NAME: {SERVICE_NAME}",flush=True)
    print(f"PROJECT_NAME: {PROJECT_NAME}",flush=True)
    print(f"NGINX_CONTAINER_NAME: {NGINX_CONTAINER_NAME}",flush=True)
    print(f"LOCAL_NETWORK: {LOCAL_NETWORK}",flush=True)

    if not isOnlyRestart:
        run_command(f"docker tag {NEW_IMAGE} {LATEST_IMAGE}")

    old_container_id = run_command(f"docker ps -f name={PROJECT_NAME}-{SERVICE_NAME} -q").stdout.strip().split('\n')[-1]
    print(f"Old container ID: {old_container_id}",flush=True)
    if not old_container_id:
        print('сервис не работал, поднимаем',flush=True)
        with open("./services/nginx/upstream.conf",'w') as f:
            f.write('''
            upstream app_server {
                server '''+SERVICE_NAME+''':'''+APP_PORT+''';
            }
        ''')
        run_command(f"docker compose -f {CONFIG_FILE} up -d --force-recreate")
        os._exit(0)

    RUNNING_CONTAINERS= run_command("docker ps --format '{{.Names}}'")
    RUNNING_CONTAINERS = RUNNING_CONTAINERS.stdout.replace("'","").split('\n')
    RUNNING_CONTAINERS.remove('')

    ALL_SERVICES = run_command(f"docker compose -f {CONFIG_FILE} config --services")
    ALL_SERVICES = ALL_SERVICES.stdout.split('\n')

    SERVICES_REMOVE = ['app','']
    for SERVICE in SERVICES_REMOVE:
        ALL_SERVICES.remove(SERVICE)

    for SERVICE in ALL_SERVICES:
        if f"{PROJECT_NAME}-{SERVICE}" not in RUNNING_CONTAINERS:
            print(f"Запуск {SERVICE}")
            run_command(f"docker compose -f {CONFIG_FILE} up -d --no-deps --scale {SERVICE}=1 --no-recreate {SERVICE} --build")
    
    if build:
        run_command(f"docker compose -f {CONFIG_FILE} up -d --no-deps --scale {SERVICE_NAME}=2 --no-recreate {SERVICE_NAME} --build")
    else:
        run_command(f"docker compose -f {CONFIG_FILE} up -d --no-deps --scale {SERVICE_NAME}=2 --no-recreate {SERVICE_NAME}")

    new_container_id = run_command(f"docker ps -f name={PROJECT_NAME}-{SERVICE_NAME} -q").stdout.strip().split('\n')[0]
    print(f"New container ID: {new_container_id}",flush=True)

    old_container_host_name = json.loads(run_command(f"docker inspect {old_container_id}").stdout)[0]['NetworkSettings']['Networks'][LOCAL_NETWORK]['DNSNames'][0]
    print(f"Old container HostName: {old_container_host_name}",flush=True)

    new_container_host_name = json.loads(run_command(f"docker inspect {new_container_id}").stdout)[0]['NetworkSettings']['Networks'][LOCAL_NETWORK]['DNSNames'][0]
    print(f"New container HostName: {new_container_host_name}",flush=True)
    
    start = time.time()
    result = run_command_no_raise(f"docker exec {NGINX_CONTAINER_NAME} curl -s -i --retry-connrefused --retry 60 --retry-delay 1 --max-time 30 --connect-timeout 30 --fail http://{new_container_host_name}:{APP_PORT}/")
    print(time.time() - start,flush=True)
    print("-\n",(run_command(f"docker logs {new_container_host_name}",Text=False).stdout.decode('utf-8')),"-")
    
    if result.returncode == 0:
        print("Start, update",flush=True)
        with open("./services/nginx/upstream.conf",'w') as f:
            f.write('''
                upstream app_server {
                    server '''+new_container_host_name+''':'''+APP_PORT+''';
                    server '''+SERVICE_NAME+''':'''+APP_PORT+''' backup;
                }
            ''')
        run_command(f'docker exec {NGINX_CONTAINER_NAME} nginx -s reload')
        
        run_command(f"docker kill --signal=SIGTERM {old_container_id}")
        run_command(f"docker wait {old_container_id}")
        run_command(f"docker rm {old_container_id}")

        run_command(f"docker compose -f {CONFIG_FILE} up -d --no-deps --scale {SERVICE_NAME}=1 --no-recreate {SERVICE_NAME}")
    else:
        print("No start, rollback",flush=True)
        if not isOnlyRestart and OLD_IMAGE is not None:
            run_command(f"docker tag {OLD_IMAGE} {LATEST_IMAGE}")
        run_command(f"docker stop {new_container_id}")
        run_command(f"docker wait {new_container_id}")
        run_command(f"docker rm {new_container_id}")

        run_command(f"docker compose -f {CONFIG_FILE} up -d --no-deps --scale {SERVICE_NAME}=1 --no-recreate {SERVICE_NAME}")

        print("Обновление не удалось, образ не запустился",flush=True)
        os._exit(1)

if __name__ == "__main__":
    main()