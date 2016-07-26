import math
import random
import string
import logging

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

BASE_MOUNT = "/mnt"

class GCEPathMapper(cwltool.pathmapper.PathMapper):
    def __init__(self, referenced_files, bucket, output):
        self._pathmap = {}
        log.debug("PATHMAPPER: " + output)
        for src in referenced_files:
            log.debug(src)
            if src.startswith('gs://'):
                ab = src
                iiib = src.split('/')[-1]
                self._pathmap[iiib] = (iiib, ab)
            else:
                ab = 'gs://' + bucket + '/' + output + '/' + src
                self._pathmap[src] = (ab, ab)

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
        self.callback(self.outputs)

class GCEPipelineJob(PipelineJob):
    def __init__(self, spec, pipeline):
        super(GCEPipelineJob, self).__init__(spec, pipeline)
        self.running = False
        
    def run(self, dry_run=False, pull_image=True, **kwargs):
        id = self.spec['id']
        mount = self.pipeline.config.get('mount-point', BASE_MOUNT)
        log.debug(self.spec)

        container = self.find_docker_requirement()

        input_ids = [input['id'].replace(id + '#', '') for input in self.spec['inputs']]
        inputs = {input: self.builder.job[input]['path'] for input in input_ids}
        
        output_path = self.pipeline.config['output-path']
        outputs = {output['id'].replace(id + '#', ''): output['outputBinding']['glob'] for output in self.spec['outputs']}

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
        collected = {output: {'path': outputs[output], 'class': 'File', 'hostfs': False} for output in outputs}
        log.debug(collected)

        interval = math.ceil(random.random() * 5 + 5)
        poll = GCEPipelinePoll(self.pipeline.service, operation, collected, lambda outputs: self.output_callback(outputs, 'success'), interval)
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
        
