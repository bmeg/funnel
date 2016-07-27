import math
import random
import string
import logging
from pprint import pformat

import cwltool.draft2tool
from cwltool.pathmapper import MapperEnt

from poll import PollThread
from pipeline import Pipeline, PipelineJob

log = logging.getLogger('funnel')

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
    # def __init__(self, referenced_files, bucket, output):
    #     super(GCEPathMapper, self).__init__(referenced_files, )

    # def setup(self, referenced_files, basedir):
    #     log.debug("PATHMAPPER SETUP" + pformat(referenced_files))
        


    def __init__(self, referenced_files, bucket, output):
        self._pathmap = {}
        log.debug("PATHMAPPER: " + pformat(output))
        for src in referenced_files:
            log.debug(pformat(src))
            base = extract_gs(src['location'])

            if base.startswith('gs://'):
                ab = base
                iiib = base.split('/')[-1]
                self._pathmap[src['location']] = (iiib, ab)
            else:
                ab = 'gs://' + bucket + '/' + output + '/' + base
                self._pathmap[src['location']] = (ab, ab)

            self._pathmap[ab] = (ab, ab)

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

class GCEPipelineJob(PipelineJob):
    def __init__(self, spec, pipeline):
        super(GCEPipelineJob, self).__init__(spec, pipeline)
        self.running = False
        
    def run(self, dry_run=False, pull_image=True, **kwargs):
        id = self.spec['id']
        mount = self.pipeline.config.get('mount-point', BASE_MOUNT)

        log.debug('SPEC ------------------')
        log.debug(pformat(self.spec))

        container = self.find_docker_requirement()
        input_ids = [input['id'].replace(id + '#', '') for input in self.spec['inputs']]
        inputs = {input: self.builder.job[input]['path'] for input in input_ids}
        
        output_path = self.pipeline.config['output-path']
        outputs = {output['id'].replace(id + '#', ''): output['outputBinding']['glob'] for output in self.spec['outputs']}

        log.debug('OUTPUTS ------------' + pformat(outputs))

        command_parts = self.spec['baseCommand'][:]
        if 'arguments' in self.spec:
            command_parts.extend(self.spec['arguments'])

        for input in self.spec['inputs']:
            input_id = input['id'].replace(id + '#', '')
            path = mount + '/' + self.builder.job[input_id]['path'].replace('gs://', '')
            command_parts.append(path)

        command = string.join(command_parts, ' ')
                
        if self.spec['stdout']:
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
        
        operation = self.pipeline.run_task(task)
        collected = {output: {
            'path': outputs[output],
            'class': 'File',
            'hostfs': False
        } for output in outputs}

        log.debug('COLLECTED OUTPUTS ------------------' + pformat(collected))

        def callback(operation, outputs):
            out = operation['metadata']['request']['pipelineArgs']['outputs']
            for key in out:
                path = out[key]
                outputs[key]['location'] = path

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

    def make_exec_tool(self, spec, **kwargs):
        return GCEPipelineTool(spec, self, **kwargs)

    def create_parameters(self, puts, replace=False):
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

    def create_task(self, project_id, container, service_account, bucket, command, inputs, outputs, output_path, mount):
        input_parameters = self.create_parameters(inputs, True)
        output_parameters = self.create_parameters(outputs)
        
        create_body = {
            'ephemeralPipeline': {
                'projectId': project_id,
                'name': 'funnel workflow',
                'description': 'run a google pipeline from cwl',
                
                'docker' : {
                    'cmd': command,
                    'imageName': container # 'gcr.io/' + project_id + '/' + container
                },
                
                'inputParameters' : input_parameters,
                'outputParameters' : output_parameters,
                
                'resources' : {
                    'disks': [{
                        'name': 'data',
                        'autoDelete': True,
                        'mountPoint': mount,
                        'sizeGb': 10,
                        'type': 'PERSISTENT_HDD',
                    }],
                    'minimumCpuCores': 1,
                    'minimumRamGb': 1,
                }
            },
                
            'pipelineArgs' : {
                'inputs': inputs,
                'outputs': {output: 'gs://' + bucket + '/' + output_path + '/' + outputs[output] for output in outputs},
                
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
        
