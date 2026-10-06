#!/usr/bin/env python3
"""Read-only NAS cache status. Run with codex-secret run chuan-nas -- python3 ... ."""
import json
import os
import shlex
import subprocess
import sys

REMOTE = r'''
import json,os,shutil,subprocess,time,urllib.request
state=json.loads(subprocess.check_output(['/usr/local/bin/docker','inspect','keeps-control-plane']))[0]
env=dict(x.split('=',1) for x in state['Config']['Env'] if '=' in x)
library=env.get('KEEPS_LIBRARY_ID','local-library')
request=urllib.request.Request('http://127.0.0.1:2283/libraries/'+library+'/cache-status',headers={'Authorization':'Bearer '+env['KEEPS_ACCESS_TOKEN']})
with urllib.request.urlopen(request,timeout=30) as response: progress=json.load(response)
root=next(m['Source'] for m in state['Mounts'] if m['Destination']==env['KEEPS_ROOT'])
disk=shutil.disk_usage(root)
result={'observedAtUTC':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'container':{'image':state['Image'],'status':state['State']['Status'],'health':state['State'].get('Health',{}).get('Status'),'restartCount':state['RestartCount'],'oomKilled':state['State'].get('OOMKilled'),'cpuSet':state['HostConfig'].get('CpusetCpus'),'memoryLimitBytes':state['HostConfig'].get('Memory')},'disk':{'freeBytes':disk.free,'totalBytes':disk.total},'cache':progress}
result['resourceUsage']=json.loads(subprocess.check_output(['/usr/local/bin/docker','stats','--no-stream','--format','{{json .}}','keeps-control-plane'],text=True))
result['nasLoadAverage']=os.getloadavg()
print(json.dumps(result,ensure_ascii=False,indent=2))
'''

def main():
    password = os.environ.get('CODEX_SSH_PASSWORD')
    if not password:
        raise SystemExit('Run through codex-secret run chuan-nas; never pass a password as an argument.')
    env = os.environ.copy()
    env.pop('CODEX_SSH_PASSWORD', None)
    result = subprocess.run(
        ['codex-secret', 'ssh', 'chuan-nas', '--', 'sudo', '-S', '-p', "''", 'python3', '-c', shlex.quote(REMOTE)],
        input=password + '\n', text=True, env=env, capture_output=True,
    )
    if result.returncode:
        print(result.stderr.replace(password, '[redacted]'), file=sys.stderr)
        raise SystemExit(result.returncode)
    payload = json.loads(result.stdout)
    print(json.dumps(payload, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
