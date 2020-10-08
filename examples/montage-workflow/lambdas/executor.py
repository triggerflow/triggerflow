import subprocess
import boto3
import json
import os
import ssl
from concurrent import futures
import xml.etree.ElementTree as ET

from MontagePy.main import mProject, mBgModel, mImgtbl, mViewer
from triggerflow_handler import triggerflow

@triggerflow
def lambda_handler(event, context):
    print(event)
    target_dir = os.getenv('TARGET_DIR')
    prefix = event['prefix']
    bucket, region = os.getenv('BUCKET'), os.getenv('REGION')

    os.environ['PATH'] = '/var/task/bin/' + ':' + os.environ['PATH']
    os.chdir(target_dir)

    s3 = boto3.client('s3', region_name=region)

    if 'command' not in event and 'job_id' in event:
        req = s3.get_object(Bucket=os.getenv('BUCKET'),
                            Key='/'.join([prefix, event['dax']]))
        root = ET.fromstring(req['Body'].read())

        job = [t for t in root.iter('{http://pegasus.isi.edu/schema/DAX}job')
                if t.attrib['id'] == event['job_id']].pop()
        
        task = {'input': [], 'output': []}
        command = job.attrib['name']
        for child in job:
            if child.tag == '{http://pegasus.isi.edu/schema/DAX}argument':
                cmd = []
                for txt, file in zip(child.itertext(), list(child)):
                    t = txt.strip()
                    if t != '':
                        cmd.extend(t.split(' '))
                    cmd.append(file.attrib['name'])
                print(cmd)
                if command == 'mConcatFit':
                    task['command'] = 'mConcatFit'
                    task['args'] = {'stats_file': cmd[0], 'fits_file': cmd[2], 'statdir': cmd[1]}
                elif command == 'mBgModel':
                    task['command'] = 'mBgModel'
                    task['args'] = {
                        'input_file': cmd[2],
                        'fit_file': cmd[3],
                        'corr_file': cmd[4],
                        'niteration': int(cmd[1]) 
                    }
                elif command == 'mImgtbl':
                    task['command'] = 'mImgtbl'
                    task['args'] = {
                        'pathname': cmd[0],
                        'tblname': cmd[3],
                        'imgListFile': cmd[2]
                    }
                elif command == 'mAdd':
                    task['command'] = 'mAdd'
                    task['args'] = {
                        'images_table': cmd[1],
                        'template_file': cmd[2],
                        'output_file': cmd[3]
                    }
                elif command == 'mViewer':
                    task['command'] = 'mViewer'
                    task['args'] = {
                        'cmdstr': ' '.join(cmd[:-2]),
                        'outFile': cmd[-1],
                        'mode': 2
                    }
                else:
                    raise Exception('Wrong command {}'.format(command))
            if child.tag == '{http://pegasus.isi.edu/schema/DAX}uses':
                if child.attrib['link'] == 'input':
                    task['input'].append(child.attrib['name'])
                elif child.attrib['link'] == 'output':
                    task['output'].append(child.attrib['name'])
        event.update(task)
        print(task['args'])

    files = os.listdir(target_dir)
    remaining_output = set(event['output']).difference(set(files))

    if not remaining_output:
        return

    remaining_input = set(event['input']).difference(set(files))

    with futures.ThreadPoolExecutor(max_workers=64) as pool:
        def download_file(input_file):
            retry = 0
            key = '/'.join([prefix, input_file])

            while retry <= 5:
                print('Going to download file {}'.format(key))
                try:
                    s3.download_file(
                        bucket, key, '/'.join([target_dir, input_file]))
                    print('Completed download of file {}'.format(key))
                    break
                except ssl.SSLError:
                    print(
                        'Failed to download file {} -- attempt {} '.format(key, retry))
                    retry += 1

        fut = []
        for inf in remaining_input:
            f = pool.submit(download_file, inf)
            fut.append(f)
        res = [f.result() for f in fut]

    command = event['command']
    if command == 'mProject':
        sta = mProject(**event['args'])
    elif command == 'mDiffFit':
        status_file = event['args']['output_file'] \
            .replace('diff', 'fit').replace('fits', 'txt')
        cmd = [
            '/var/task/bin/mDiffFit', '-d', '-s', status_file,
            event['args']['input_file1'], event['args']['input_file2'],
            event['args']['output_file'], event['args']['template_file']]
        print(cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=target_dir)
        sta, error = proc.communicate()
        print(error)
    elif command == 'mConcatFit':
        cmd = [
            '/var/task/bin/mConcatFit', event['args']['stats_file'],
            event['args']['fits_file'], event['args']['statdir']]
        print(cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=target_dir)
        sta, error = proc.communicate()
        print(error)
    elif command == 'mBgModel':
        sta = mBgModel(**event['args'])
    elif command == 'mBackground':
        cmd = [
            '/var/task/bin/mBackground', '-t',
            event['args']['input_file'], event['args']['output_file'],
            event['args']['images_table'], event['args']['corrected_table']
        ]
        print(cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=target_dir)
        sta, error = proc.communicate()
        print(error)
    elif command == 'mImgtbl':
        sta = mImgtbl(**event['args'])
    elif command == 'mAdd':
        cmd = [
            '/var/task/bin/mAdd', '-e', event['args']['images_table'],
            event['args']['template_file'], event['args']['output_file']
        ]
        print(cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=target_dir)
        sta, error = proc.communicate()
        print(error)
    elif command == 'mViewer':
        sta = mViewer(**event['args'])
    else:
        raise Exception('Command {} not found'.format(command))
    print(sta)

    if 'result' in event:
        output_file = event['result']
        key = '/'.join([prefix, output_file])
        s3.upload_file(os.path.join(target_dir, output_file), bucket, key)

    return {'result': 'ok'}
