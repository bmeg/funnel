import os
import shutil
import logging

import cwltool.draft2tool
from cwltool.pathmapper import MapperEnt

from poll import PollThread
from pipeline import Pipeline, PipelineJob

try:
    import requests
except ImportError:
    pass

log = logging.getLogger('funnel')
BASE_MOUNT = "/mnt"

class LocalStorePathMapper(cwltool.pathmapper.PathMapper):
    def __init__(self, referenced_files, basedir, store_base, **kwargs):
        self.store_base = store_base
        self.setup(referenced_files, basedir)

    def setup(self, referenced_files, basedir):
        self._pathmap = {}
        for src in referenced_files:
            logging.debug(src)
            if src['location'].startswith("fs://"):
                target_name = os.path.basename(src['location'])
                self._pathmap[src['location']] = MapperEnt(
                    resolved=src['location'],
                    target=os.path.join(BASE_MOUNT, target_name),
                    type=src['class']
                )
            elif src['location'].startswith("file://"):
                src_path = src['location'][7:]
                logging.debug("Copying %s to shared %s" % (src['location'], self.store_base))
                dst = os.path.join(self.store_base, os.path.basename(src_path))
                shutil.copy(src_path, dst)
                location = "fs://%s" % (os.path.basename(src['location']))
                self._pathmap[src['location']] = MapperEnt(
                    resolved=location,
                    target=os.path.join(BASE_MOUNT, os.path.basename(src['location'])),
                    type=src['class']
                )
            else:
                raise Exception("Unknown file source: %s" %(src['location']))

class TESService:
    def __init__(self, addr):
        self.addr = addr

    def submit(self, task):
        r = requests.post("%s/v1/jobs" % (self.addr), json=task)
        data = r.json()
        if 'Error' in data:
            raise Exception("Request Error: %s" % (data['Error']) )
        return data['value']

    def get_job(self, job_id):
        r = requests.get("%s/v1/jobs/%s" % (self.addr, job_id))
        return r.json()

    def get_server_metadata(self):
        r = requests.get("%s/v1/jobs-service" % (self.addr))
        return r.json()

class TESPipeline(Pipeline):
    def __init__(self, config):
        super(TESPipeline, self).__init__(config)
        self.service = TESService(config['url'])

    def create_parameters(self, puts, pathmapper):
        parameters = []
        for put in puts:
            path = puts[put]
            rev = pathmapper.reversemap(path)
            if rev is not None:
                parameter = {
                    'name': put,
                    'description': put,
                    'location' : rev[1],
                    'path': path
                }
                parameters.append(parameter)

        return parameters

    def create_task(self, container, command, inputs, outputs, volumes, config, pathmapper, stdout=None, stderr=None):
        input_parameters = self.create_parameters(inputs, pathmapper)
        output_parameters = self.create_parameters(outputs, pathmapper)

        create_body = {
            'projectId': "test",
            'name': 'funnel workflow',
            'description': 'CWL TES task',
            'docker' : [{
                'cmd': command,
                'imageName': container
            }],
            'inputs' : input_parameters,
            'outputs' : output_parameters,
            'resources' : {
                'volumes': [{
                    'name': 'data',
                    'mountPoint': BASE_MOUNT,
                    'sizeGb': 10,
                }],
                'minimumCpuCores': 1,
                'minimumRamGb': 1,
            }
        }
            
        if stdout is not None:
            create_body['docker'][0]['stdout'] = stdout[0]
            parameter = {
                'name': 'stdout',
                'description': 'tool stdout',
                'location' : stdout[1],
                'path': stdout[0]
            }
            create_body['outputs'].append(parameter)
                    
        if stderr is not None:
            create_body['docker'][0]['stderr'] = stderr[0]
            parameter = {
                'name': 'stderr',
                'description': 'tool stderr',
                'location' : stderr[1],
                'path': stderr[0]
            }
            create_body['outputs'].append(parameter)

        #print "task", create_body
        return create_body

    def make_exec_tool(self, spec, **kwargs):
        return TESPipelineTool(spec, self, **kwargs)

class TESPipelineTool(cwltool.draft2tool.CommandLineTool):
    def __init__(self, spec, pipeline, **kwargs):
        super(TESPipelineTool, self).__init__(spec, **kwargs)
        self.spec = spec
        self.pipeline = pipeline
    
    def makeJobRunner(self):
        return TESPipelineJob(self.spec, self.pipeline)

    def makePathMapper(self, reffiles, stagedir, **kwargs):
        m = self.pipeline.service.get_server_metadata()
        if m['storageConfig'].get('storageType', "") == "sharedFile":
            return LocalStorePathMapper(reffiles, store_base=m['storageConfig']['baseDir'], **kwargs)

class TESPipelineJob(PipelineJob):
    def __init__(self, spec, pipeline):
        super(TESPipelineJob,self).__init__(spec, pipeline)
        self.running = False
        
    def run(self, dry_run=False, pull_image=True, **kwargs):
        id = self.spec['id']
        print self.spec['inputs']
        print self.build.job
        input_ids = [input['id'].replace(id + '#', '') for input in self.spec['inputs']]
        inputs = {input: self.builder.job[input]['path'] for input in input_ids}
        
        output_path = self.pipeline.config['output-path']
        outputs = {output['id'].replace(id + '#', ''): output['outputBinding']['glob'] for output in self.spec['outputs']}

        command_parts = self.spec['baseCommand'][:]
        if 'arguments' in self.spec:
            command_parts.extend(self.spec['arguments'])

        for input in self.spec['inputs']:
            input_id = input['id'].replace(id + '#', '')
            path = os.path.join( self.builder.job[input_id]['path'] )
            command_parts.append(path)
        
        
        stdout_path=self.spec.get('stdout', None)
        stderr_path=self.spec.get('stderr', None)
        
        if stdout_path is not None:
            stdout = (self.output2path(stdout_path), self.output2location(stdout_path))
        else:
            stdout = None
        if stderr_path is not None:
            stderr = (self.output2path(stderr_path), self.output2location(stderr_path))
        else:
            stderr = None
        
        container = self.find_docker_requirement()

        log.debug(self.pathmapper)
        task = self.pipeline.create_task(
            container=container,
            command=command_parts,
            inputs=inputs,
            outputs=outputs,
            volumes=BASE_MOUNT,
            config=self.pipeline.config,
            pathmapper=self.pathmapper,
            stderr=stderr,
            stdout=stdout
        )
        
        task = self.pipeline.service.submit(task)
        operation = self.pipeline.service.get_job(task)
        collected = {output: {'location': "fs://output/" + outputs[output], 'class': 'File', 'hostfs': False} for output in outputs}

        log.debug("op", operation)
        log.debug(collected)

        poll = TESPipelinePoll(
            service=self.pipeline.service,
            operation=operation,
            outputs=collected,
            callback=lambda outputs: self.output_callback(outputs, 'success')
        )
        self.pipeline.add_thread(poll)
        poll.start()
    
    def output2location(self, path):
        return "fs://output/" + os.path.basename(path)
        
    def output2path(self, path):
        return "/mnt/" + path    


class TESPipelinePoll(PollThread):
    def __init__(self, service, operation, outputs, callback):
        super(TESPipelinePoll, self).__init__(operation)
        self.service = service
        self.outputs = outputs
        self.callback = callback

    def poll(self):
        return self.service.get_job(self.operation['jobId'])

    def is_done(self, operation):
        return operation['state'] in ['Complete', 'Error']

    def complete(self, operation):
        self.callback(self.outputs)

