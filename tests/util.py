import shlex
import subprocess
import requests

TEST_CORE = 'testcore'


def start_solr():
    """Bring up a container running ES.

    Waits until REST API is live and responsive.
    """
    print('Starting SOLR server...')

    cmd = shlex.split('docker run -d --name intake-solr -p 8983:8983 -P '
                      'solr solr-create -c ' + TEST_CORE)
    cid = subprocess.check_output(cmd).decode()[:-1]

    while True:
        try:
            r = requests.get('http://localhost:8983')
            if r.ok:
                break
        except requests.ConnectionError:
            pass
    return cid


def stop_docker(name='intake-solr', cid=None, let_fail=False):
    """Stop docker container with given name tag

    Parameters
    ----------
    name: str
        name field which has been attached to the container we wish to remove
    cid: str
        container ID, if known
    let_fail: bool
        whether to raise an exception if the underlying commands return an
        error.
    """
    try:
        if cid is None:
            print('Finding %s ...' % name)
            cmd = shlex.split('docker ps -q --filter "name=%s"' % name)
            cid = subprocess.check_output(cmd).strip().decode()
        if cid:
            print('Stopping %s ...' % cid)
            subprocess.call(['docker', 'kill', cid])
            subprocess.call(['docker', 'rm', cid])
    except subprocess.CalledProcessError as e:
        print(e)
        if not let_fail:
            raise
