import json
import boto3
import os
import xml.etree.ElementTree as ET


def lambda_handler(event, context):
    prefix, dax, branch = event['prefix'], event['dax'], event['branch']
    s3 = boto3.client('s3', region_name=os.getenv('REGION'))
    req = s3.get_object(Bucket=os.getenv('BUCKET'), Key='/'.join([prefix, dax]))
    root = ET.fromstring(req['Body'].read())
    step = event['step']
    
    tasks = []
    jobs = []
    for job in root.iter('{http://pegasus.isi.edu/schema/DAX}job'):
        if job.attrib['name'] == step:
            branch_contains = []
            for child in job.iter('{http://pegasus.isi.edu/schema/DAX}uses'):
                branch_contains.append('name' in child.attrib and branch in child.attrib['name'])
            if any(branch_contains):
                jobs.append(job)
    
    for job in jobs:
        task = {
            'command': step,
            'args': {},
            'input': [],
            'output': [],
            'indir': '',
            'outdir': '',
            'prefix': prefix,
        }
    
        for child in job:
            if child.tag == '{http://pegasus.isi.edu/schema/DAX}argument':
                command = [job.attrib['name']]
                for txt, file in zip(child.itertext(), list(child)):
                    if (t := txt.strip()) != '':
                        command.extend(t.split(' '))
                    command.append(file.attrib['name'])
                if step == 'mProject':
                    args = {
                        'input_file': command[2],
                        'output_file': command[3],
                        'template_file': command[4],
                        'expand': True
                    }
                    task['indir'] = '/'.join(['images', branch])
                    task['outdir'] = '/'.join(['projected', branch])
                elif step == 'mDiffFit':
                    args = {
                        'input_file1': command[4],
                        'input_file2': command[5],
                        'output_file': command[6],
                        'template_file': command[7] 
                    }
                    task['indir'] = '/'.join(['projected', branch])
                    task['outdir'] = '/'.join(['diff', branch])
                elif step == 'mBackground':
                    args = {
                        'input_file': command[2],
                        'output_file': command[3],
                        'images_table': command[4],
                        'corrected_table': command[5]
                    }
                    task['indir'] = '/'.join(['bgmodel', branch])
                    task['outdir'] = '/'.join(['background', branch])
                task['args'] = args
            if child.tag == '{http://pegasus.isi.edu/schema/DAX}uses':
                if child.attrib['link'] == 'input':
                    task['input'].append(child.attrib['name'])
                elif child.attrib['link'] == 'output':
                    task['output'].append(child.attrib['name'])
        
        tasks.append(task)
    
    return tasks