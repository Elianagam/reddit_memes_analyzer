import re
import socket
import subprocess

ID_REGEX = ".*_(?P<id>\d*)$"
CONTAINER_REGEX = ".*\/(?P<container_name>[\w_]*_\d*)(\\n)?"


def get_hostname():
    return socket.gethostname()


def get_container_name():
    hostname = get_hostname()
    cmd = ["docker", "inspect", "-f", "{{.Name}}", hostname]
    cmd_result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = cmd_result.stdout.decode()
    m = re.match(CONTAINER_REGEX, output)
    if m:
        data = m.groupdict()
        return data['container_name']


def start_container(name):
    result = subprocess.run(['docker', 'start', name],
                            check=False,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)


def get_node_id():
    node_name = get_container_name()
    m = re.match(ID_REGEX, node_name)
    if m:
        data = m.groupdict()
        return int(data['id'])
    return 1
