import os
import math
import random
import string
import logging
from pprint import pformat

import cwltool.draft2tool
from cwltool.pathmapper import MapperEnt

from poll import PollThread
from pipeline import Pipeline, PipelineJob
from gce_fsaccess import GCEFsAccess

log = logging.getLogger('funnel')

import httplib2shim
httplib2shim.patch()

try:
    from oauth2client.client import GoogleCredentials
    from apiclient.discovery import build
except ImportError:
    pass

BASE_MOUNT = "/mnt/data"

def find_index(s, sub):
    try:
        return s.index(sub)
    except:
        None

def extract_gs(s):
    index = find_index(s, 'gs:/')
    if index:
        gs = s[index:]
        return gs[:4] + '/' + gs[4:]
    else:
        return s

class GCEPathMapper(cwltool.pathmapper.PathMapper):
    def __init__(self, referenced_files, bucket, output):
        self._pathmap = {}
        log.debug("PATHMAPPER: " + pformat(output))
        for src in referenced_files:
            log.debug(pformat(src))
            base = extract_gs(src['location'])

            if base.startswith('gs://'):
                ab = base
                ooob = base.replace('gs://', '')
                iiib = base.split('/')[-1]
                self._pathmap[src['location']] = (iiib, ooob)
            else:
                ab = 'gs://' + bucket + '/' + output + '/' + base
                self._pathmap[src['location']] = (ab, ab)

            self._pathmap[ab] = (ab, ab.replace('gs://', ''))

class GCEPipelinePoll(PollThread):
    def __init__(self, service, operation, outputs, callback, poll_interval=5):
        super(GCEPipelinePoll, self).__init__(operation, poll_interval)
        self.service = service
        self.outputs = outputs
        self.callback = callback

    def poll(self):
        return self.service.operations().get(name=self.operation['name']).execute()

    def is_done(self, operation):
        return operation['done']

    def complete(self, operation):
        self.callback(operation, self.outputs)

def dictify(lot):
    return {k: v for k, v in lot}

def input_binding(input, key, default):
    position = default
    if key in input:
        position = input[key]
    elif 'inputBinding' in input:
        if key in input['inputBinding']:
            position = input['inputBinding'][key]

    return position

class GCEPipelineJob(PipelineJob):
    def __init__(self, spec, pipeline):
        super(GCEPipelineJob, self).__init__(spec, pipeline)
        self.running = False
        
    def render_output(self, id, output):
        glob = output['outputBinding']['glob']
        if glob.startswith('$'):
            glob = self.builder.do_eval(glob)
        if output['type'] == 'File':
            path = (output['id'].replace(id + '#', ''), glob)
        elif output['type'] == 'Directory':
            if glob == '.':
                glob = '/*'
            elif not glob[-1] == '/':
                glob = glob + '/*'
            path = (output['id'].replace(id + '#', ''), glob)
        else:
            path = (output['id'].replace(id + '#', ''), glob)
        return path

    def fill_path(self, path, entry):
        base = os.path.basename(entry)
        parts = path.split('/')
        if parts[-1] == '*':
            parts = parts[:-1]
        parts.append(base)
        return os.path.join(*parts)

    def explode(self, output, fs):
        contents = fs.listdir(output['location'])

        log.debug('BUCKET CONTENTS ---------------------------------' + output['location'])
        log.debug(pformat(contents))

        return [{'location': entry,
                 'class': 'File',
                 'path': self.fill_path(output['path'], entry)} for entry in contents]

    def is_collection(self, u):
        return isinstance(self.joborder[u], list) or isinstance(self.joborder[u], dict)

    def prepare_input(self, tuples, key):
        input = self.joborder[key]
        if isinstance(input, dict):
            tuples.append((key, 'gs://' + input['path']))
        else:
            tuples.extend([(key + '_' + put['basename'], 'gs://' + put['path']) for put in input])
        return tuples

    def run(self, dry_run=False, pull_image=True, **kwargs):
        id = self.spec['id']
        mount = self.pipeline.config.get('mount-point', BASE_MOUNT)

        log.debug('SPEC ------------------')
        log.debug(pformat(self.spec))

        log.debug('COMMAND ----------------------')
        log.debug(pformat(self.command_line))

        log.debug('DIR JOB ----------------------')
        log.debug(pformat(dir(self)))

        log.debug('JOBORDER ----------------------')
        log.debug(pformat(self.joborder))

        container = self.find_docker_requirement()
        inputs = dictify(reduce(self.prepare_input, filter(self.is_collection, self.joborder), []))

        output_path = self.pipeline.config['output-path']
        spec_outputs = {output['id'].replace(id + '#', ''): output for output in self.spec['outputs']}
        outputs = dictify([self.render_output(id, output) for output in self.spec['outputs']])

        log.debug('SPEC OUTPUTS ------------' + pformat(spec_outputs))
        log.debug('OUTPUTS ------------' + pformat(outputs))

        command_parts = ['cd', mount, '&&'] + self.command_line
        command = string.join(command_parts, ' ')
                
        if 'stdout' in self.spec:
            command += ' > ' + mount + '/' + self.spec['stdout']

        task = self.pipeline.create_task(
            self.pipeline.config['project-id'],
            container,
            self.pipeline.config['service-account'],
            self.pipeline.config['bucket'],
            command,
            inputs,
            outputs,
            output_path,
            mount
        )
        
        log.debug('SUBMITTED TASK -----------------------------------')
        log.debug(pformat(task))

        operation = self.pipeline.run_task(task)
        collected = {output: {
            'path': outputs[output],
            # 'Directory' if outputs[output][-1] == '/' else 'File',
            'class': spec_outputs[output]['type'],
            'hostfs': False
        } for output in outputs}

        log.debug('COLLECTED OUTPUTS ------------------' + pformat(collected))

        def callback(operation, outputs):
            out = operation['metadata']['request']['pipelineArgs']['outputs']
            for key in out:
                path = out[key]
                outputs[key]['location'] = path

                if isinstance(outputs[key]['class'], dict):
                    outputs[key] = self.explode(outputs[key], kwargs['fs_access'])

            log.debug('FINAL OUTPUT -----------------------------------')
            log.debug(pformat(outputs))

            self.output_callback(outputs, 'success')

        interval = math.ceil(random.random() * 5 + 5)
        poll = GCEPipelinePoll(self.pipeline.service, operation, collected, callback, interval)
        poll.start()

class GCEPipelineTool(cwltool.draft2tool.CommandLineTool):
    def __init__(self, spec, pipeline, **kwargs):
        super(GCEPipelineTool, self).__init__(spec, **kwargs)
        self.spec = spec
        self.pipeline = pipeline
        
    def makeJobRunner(self):
        return GCEPipelineJob(self.spec, self.pipeline)

    def makePathMapper(self, reffiles, stagedir, **kwargs):
        return GCEPathMapper(reffiles, self.pipeline.config['bucket'], self.pipeline.config['output-path'])

class GCEPipeline(Pipeline):
    def __init__(self, config):
        super(GCEPipeline, self).__init__(config)
        self.credentials = GoogleCredentials.get_application_default()
        self.service = build('genomics', 'v1alpha2', credentials=self.credentials)

    def configure(self, args):
        args['fs_access'] = GCEFsAccess(self.config['bucket'])
        return args

    def make_exec_tool(self, spec, **kwargs):
        return GCEPipelineTool(spec, self, **kwargs)

    def create_parameters(self, puts, replace=True):
        parameters = []
        for put in puts:
            path = puts[put]
            if replace:
                path = path.replace('gs://', '')

            parameter = {
                'name': put,
                'description': put,
                'localCopy': {
                    'path': path,
                    'disk': 'data'
                }
            }
            parameters.append(parameter)

        return parameters

    def input_command(self, input_parameters):
        command = ['/mnt/data/' + parameter['localCopy']['path'] for parameter in input_parameters]
        return string.join(command, ' ')

    def pipeline_output(self, output):
        prefix = '' if output[0] == '/' else '/'
        if output[-1] == '*':
            output = output[:-2]
        return prefix + output

    def create_task(self, project_id, container, service_account, bucket, command, inputs, outputs, output_path, mount):
        input_parameters = self.create_parameters(inputs)
        output_parameters = self.create_parameters(outputs)
        
        create_body = {
            'ephemeralPipeline': {
                'projectId': project_id,
                'name': 'funnel workflow',
                'description': 'run a google pipeline from cwl',
                
                'docker' : {
                    'cmd': command,
                    'imageName': container
                },
                
                'inputParameters' : input_parameters,
                'outputParameters' : output_parameters,
                
                'resources' : {
                    'disks': [{
                        'name': 'data',
                        'autoDelete': True,
                        'mountPoint': mount,
                        'sizeGb': 150,
                        'type': 'PERSISTENT_HDD',
                    }],
                    'minimumCpuCores': 1,
                    'minimumRamGb': 16,
                }
            },
                
            'pipelineArgs' : {
                'inputs': inputs,
                'outputs': {output: 'gs://' + bucket + '/' + output_path + self.pipeline_output(outputs[output]) for output in outputs},
                
                'logging': {
                    'gcsPath': 'gs://' + bucket + '/' + project_id + '/logging'
                },
                
                'projectId': project_id,
                
                'serviceAccount': {
                    'email': service_account,
                    'scopes': ['https://www.googleapis.com/auth/cloud-platform']
                }
            }
        }
        
        return create_body
        
    def run_task(self, body):
        return self.service.pipelines().run(body=body).execute()
        
