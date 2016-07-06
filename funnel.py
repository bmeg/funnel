import os
import sys
import math
import time
import json
import argparse
import random
import string
import threading
import logging
import yaml

import cwltool.main
import cwltool.docker
import cwltool.process
import cwltool.workflow
import cwltool.draft2tool

from pprint import pprint
try:
    from oauth2client.client import GoogleCredentials
    from apiclient.discovery import build
except ImportError:
    pass

####
#GLOBALS
####
DEBUG=False

BASE_MOUNT = "/mnt"
DEFAULT_IMAGE = "ubuntu:15.04"

################################################################################
#Base Funnel Classes
################################################################################

class Pipeline(object):
    
    def __init__(self, engine_args):
        self.engine_args = engine_args
    
    def create_task(self, container, command, inputs, outputs, volumes, engine_args):
        """
        Given a cwl spec and job create a engine task and pass back data structure
        to use for submission
        """
        raise Exception("Not Implemented")
    
    def run_task(self, task):
        raise Exception("Not Implemented")
    

    def executor(self, tool, job_order, **kwargs):
        if DEBUG:
            pprint(kwargs)
        jobs = tool.job(job_order, self.output_callback, **kwargs)

        for runnable in jobs:
          if runnable:
            runnable.run(**kwargs)

        logging.info('all processes have joined')
        logging.info(self.output)

        return self.output

    def make_exec_tool(self, spec, **kwargs):
        raise Exception("Not Implemented")

    def make_tool(self, spec, **kwargs):
        if 'class' in spec and spec['class'] == 'CommandLineTool':
          return self.make_exec_tool(spec, **kwargs)
        else:
          return cwltool.workflow.defaultMakeTool(spec, **kwargs)

    def output_callback(self, out, status):
        if status == 'success':
            logging.info('Job completed!')
        else:
            logging.info('Job failed...')
        self.output = out

class PipelineJob(threading.Thread):
  def __init__(self, service, operation, outputs, callback, poll_interval=5):
    """
    Args:
      service : 
      taskinst_id : 
      
    """
    super(PipelinePoll, self).__init__()
    self.service = service
    self.operation = operation
    self.poll_interval = poll_interval
    self.outputs = outputs
    self.callback = callback
    self.success = None

  def poll(self):
      raise Exception("Not Implemented")

  def run(self):
    operation = self.operation
    while not operation['done']:
      time.sleep(self.poll_interval)
      logging.debug('POLLING ' + operation['name'])
      #operation = self.service.operations().get(name=operation['name']).execute()
      operation = self.ping()

    if DEBUG:
        pprint(operation)
    self.success = operation
    self.callback(self.outputs)

class PipelineJob(object):
  def __init__(self, spec, pipeline):
    self.spec = spec
    self.pipeline = pipeline
    self.running = False
    
  def run(self, dry_run=False, pull_image=True, **kwargs):
      raise Exception("Not Implemented")

class CommandJob(cwltool.job.CommandLineJob):
  def __init__(self, spec):
    super(CommandJob, self).__init__()
    self.spec = spec

  def run(self, dry_run=False, pull_image=True, **kwargs):
    this = self

    def runnnn(this, kwargs):
      logging.debug("Starting Thread")
      super(CommandJob, this).run(**kwargs)

    thread = threading.Thread(target=runnnn, args=(this, kwargs))
    thread.start()


class CommandTool(cwltool.draft2tool.CommandLineTool):
  def __init__(self, spec, **kwargs):
    super(cwltool.draft2tool.CommandLineTool, self).__init__(spec, **kwargs)
    self.spec = spec
    
  def makeJobRunner(self):
    return CommandJob(self.spec)

  def makePathMapper(self, reffiles, **kwargs):
    useDocker = False
    for i in self.spec.get("requirements", []) + self.spec.get("hints", []):
      if i.get("class", "NA") == "DockerRequirement":
        useDocker = True
    if useDocker:
      return cwltool.pathmapper.DockerPathMapper(reffiles, kwargs['basedir'])
    return cwltool.pathmapper.PathMapper(reffiles, kwargs['basedir'])



################################################################################
## GCE Pipeline API Code
################################################################################

class GCEPipeline(Pipeline):
  def __init__(self, pipeline_args):
    self.credentials = GoogleCredentials.get_application_default()
    self.service = build('genomics', 'v1alpha2', credentials=self.credentials)

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

  def create_pipeline(self, project_id, container, service_account, bucket, command, inputs, outputs, output_path, mount):
    input_parameters = self.create_parameters(inputs, True)
    output_parameters = self.create_parameters(outputs)
    
    create_body = {
      'ephemeralPipeline': {
        'projectId': project_id,
        'name': 'funnel workflow',
        'description': 'run a google pipeline from cwl',
        
        'docker' : {
          'cmd': command,
          'imageName': 'gcr.io/' + project_id + '/' + container
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
          'gcsPath': 'gs://' + bucket + '/' + project_id + '/' + container + '/logging'
        },
        
        'projectId': project_id,
        
        'serviceAccount': {
          'email': service_account,
          'scopes': ['https://www.googleapis.com/auth/cloud-platform']
        }
      }
    }
    
    return create_body
    
  def run_pipeline(self, body):
    return self.service.pipelines().run(body=body).execute()
    
  def funnel_to_pipeline(self, project_id, container, service_account, bucket, command, inputs, outputs, output_path, mount):
    body = self.create_pipeline(project_id, container, service_account, bucket, command, inputs, outputs, output_path, mount)
    if DEBUG:
        pprint(body)
    
    result = self.run_pipeline(body)
    if DEBUG:
        pprint(result)
    
    return result


class GCEPipelineJob(object):
  def __init__(self, spec, pipeline, pipeline_args):
    self.spec = spec
    self.pipeline = pipeline
    self.pipeline_args = pipeline_args
    self.running = False
    
  def run(self, dry_run=False, pull_image=True, **kwargs):
    id = self.spec['id']
    mount = '/mnt/data'
    if DEBUG:
        pprint(self.spec)

    input_ids = [input['id'].replace(id + '#', '') for input in self.spec['inputs']]
    inputs = {input: self.builder.job[input]['path'] for input in input_ids}
    
    output_path = self.pipeline_args['output-path']
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

    operation = self.pipeline.funnel_to_pipeline(
      self.pipeline_args['project-id'],
      self.pipeline_args['container'],
      self.pipeline_args['service-account'],
      self.pipeline_args['bucket'],
      command,
      inputs,
      outputs,
      output_path,
      mount
    )
    
    collected = {output: {'path': outputs[output], 'class': 'File', 'hostfs': False} for output in outputs}
    if DEBUG:
        pprint(collected)

    interval = math.ceil(random.random() * 5 + 5)
    poll = PipelinePoll(self.pipeline.service, operation, collected, lambda outputs: self.output_callback(outputs, 'success'), interval)
    poll.start()



class GCEPipelineTool(cwltool.draft2tool.CommandLineTool):
  def __init__(self, spec, pipeline, pipeline_args, **kwargs):
    super(GCEPipelineTool, self).__init__(spec, **kwargs)
    self.spec = spec
    self.pipeline = pipeline
    self.pipeline_args = pipeline_args
    
  def makeJobRunner(self):
    return PipelineJob(self.spec, self.pipeline, self.pipeline_args)

  def makePathMapper(self, reffiles, **kwargs):
    return PipelinePathMapper(reffiles, self.pipeline_args['bucket'], self.pipeline_args['output-path'])


################################################################################
##Task Execution System Code
################################################################################

class TESPipeline(Pipeline):
    def __init__(self, engine_args):
        self.engine_args = engine_args

    def create_task(self, 
        command, inputs, outputs, volumes, stdout, stderr ):
        raise Exception("FIX ME!!!!!!")

class TESPipelineTool(cwltool.draft2tool.CommandLineTool):
  def __init__(self, spec, pipeline, pipeline_args, **kwargs):
    super(TESPipelineTool, self).__init__(spec, **kwargs)
    self.spec = spec
    self.pipeline = pipeline
    self.pipeline_args = pipeline_args
  
  def makeJobRunner(self):
    return TESPipelineJob(self.spec, self.pipeline, self.pipeline_args)

  def makePathMapper(self, reffiles, **kwargs):
    return TESPipelinePathMapper(reffiles)


class TESPipelineJob(object):
  def __init__(self, spec, pipeline, pipeline_args):
    self.spec = spec
    self.pipeline = pipeline
    self.pipeline_args = pipeline_args
    self.running = False
    
  def run(self, dry_run=False, pull_image=True, **kwargs):
    id = self.spec['id']

    if DEBUG:
        pprint(self.spec)

    print self.spec

    input_ids = [input['id'].replace(id + '#', '') for input in self.spec['inputs']]
    inputs = {input: self.builder.job[input]['path'] for input in input_ids}
    
    output_path = self.pipeline_args['output-path']
    outputs = {output['id'].replace(id + '#', ''): output['outputBinding']['glob'] for output in self.spec['outputs']}

    command_parts = self.spec['baseCommand'][:]
    if 'arguments' in self.spec:
      command_parts.extend(self.spec['arguments'])

    for input in self.spec['inputs']:
      input_id = input['id'].replace(id + '#', '')
      path = os.path.join( self.builder.job[input_id]['path'] )
      command_parts.append(path)
    
    stdout=self.spec.get('stdout', "")
    stderr=self.spec.get('stderr', "")
    
    container=DEFAULT_IMAGE
    for i in self.spec.get("requirements", []) + self.spec.get("hints", []):
      if i.get("class", "NA") == "DockerRequirement":
         container = i.get("dockerPull", DEFAULT_IMAGE)

    operation = self.pipeline.funnel_to_pipeline(
      self.pipeline_args.get('project-id', ""),
      container=container,
      command=command_parts,
      inputs=inputs,
      outputs=outputs,
      output_path=output_path,
      mount=BASE_MOUNT,
      stderr=stderr,
      stdout=stdout
    )
    
    collected = {output: {'path': outputs[output], 'class': 'File', 'hostfs': False} for output in outputs}
    if DEBUG:
        pprint(collected)

    interval = math.ceil(random.random() * 5 + 5)
    poll = PipelinePoll(self.pipeline.service, operation, collected, lambda outputs: self.output_callback(outputs, 'success'), interval)
    poll.start()


################################################################################
## PathMappers
################################################################################


class GCEPathMapper(cwltool.pathmapper.PathMapper):
  def __init__(self, referenced_files, bucket, output):
    self._pathmap = {}
    logging.debug("PATHMAPPER: " + output)
    for src in referenced_files:
      logging.debug(src)
      if src.startswith('gs://'):
        ab = src
        iiib = src.split('/')[-1]
        self._pathmap[iiib] = (iiib, ab)
      else:
        ab = 'gs://' + bucket + '/' + output + '/' + src
        self._pathmap[src] = (ab, ab)

      self._pathmap[ab] = (ab, ab)


class LocalStorePathMapper(cwltool.pathmapper.PathMapper):
  def __init__(self, referenced_files):
    self._pathmap = {}
    #logging.debug("PATHMAPPER: " + output)
    for src in referenced_files:
      logging.debug(src)
      if src.startswith('gs://'):
        ab = src
        iiib = src.split('/')[-1]
        self._pathmap[iiib] = (iiib, ab)
      else:
        #ab = 'gs://' + bucket + '/' + output + '/' + src
        ab = src
        self._pathmap[src] = (ab, ab)

      self._pathmap[ab] = (ab, ab)


def main(args):
    
    parser = arg_parser()
    newargs = parser.parse_args(args)
  
    if newargs.gce is not None:
        with open(newargs.gce) as handle:
            pipeline_args = yaml.load(handle.read())
        pipeline = GCEPipeline(pipeline_args)
    elif newargs.tes is not None:
        with open(newargs.tes) as handle:
            pipeline_args = yaml.load(handle.read())
        pipeline = TESPipeline(pipeline_args)
    else:
        pipeline = LocalPipeline()
        pipeline_args = {}
  
    cwltool.main.main(args=newargs, executor=pipeline.executor, makeTool=pipeline.make_tool)


def arg_parser():  # type: () -> argparse.ArgumentParser
    parser = argparse.ArgumentParser(description='Arvados executor for Common Workflow Language')

    parser.add_argument("--basedir", type=str,
                        help="Base directory used to resolve relative references in the input, default to directory of input object file or current directory (if inputs piped/provided on command line).")
    parser.add_argument("--outdir", type=str, default=os.path.abspath('.'),
                        help="Output directory, default current directory")
    parser.add_argument("--conformance-test", action="store_true", default=False)

    parser.add_argument("--eval-timeout",
                        help="Time to wait for a Javascript expression to evaluate before giving an error, default 20s.",
                        type=float,
                        default=20)
    parser.add_argument("--version", action="store_true", help="Print version and exit")
    
    parser.add_argument("--gce", default=None, help="Google Compute Config")
    
    parser.add_argument("--tes", default=None, help="Task Execution System Config")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument("--verbose", action="store_true", help="Default logging")
    exgroup.add_argument("--quiet", action="store_true", help="Only print warnings and errors.")
    exgroup.add_argument("--debug", action="store_true", help="Print even more logging")

    parser.add_argument("--tool-help", action="store_true", help="Print command line help for tool")

    parser.add_argument("--project-uuid", type=str, help="Project that will own the workflow jobs, if not provided, will go to home project.")
    parser.add_argument("--ignore-docker-for-reuse", action="store_true",
                        help="Ignore Docker image version when deciding whether to reuse past jobs.",
                        default=False)

    parser.add_argument("workflow", type=str, nargs="?", default=None, help="The workflow to execute")
    parser.add_argument("job_order", nargs=argparse.REMAINDER, help="The input object to the workflow.")

    return parser


if __name__ == '__main__':  
    sys.exit(main(sys.argv[1:]))

