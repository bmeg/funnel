import time
import logging
import cwltool.workflow

from cwltool.errors import WorkflowException

log = logging.getLogger('funnel')
DEFAULT_IMAGE = "ubuntu:15.04"

class Pipeline(object):
    def __init__(self, config):
        self.config = config
        self.threads = []
        
    def create_task(self, container, command, inputs, outputs, volumes, config):
        """
        Given a cwl spec and job create a engine task and pass back data structure
        to use for submission
        """
        raise Exception("Pipeline.create_task() not implemented")
        
    def run_task(self, task):
        raise Exception("Pipeline.run_task() not implemented")
        
    def executor(self, tool, job_order, **kwargs):
        log.debug(kwargs)

        jobs = tool.job(job_order, self.output_callback, **kwargs)

        try:
            for runnable in jobs:
                if runnable:
                    runnable.run(**kwargs)
                else:
                    time.sleep(1)
        except WorkflowException:
            raise
        except Exception as e:
            log.exception('workflow error')
            raise WorkflowException(unicode(e))

        self.wait()
        log.info('all processes have joined')
        log.info(self.output)

        return self.output

    def make_exec_tool(self, spec, **kwargs):
        raise Exception("Pipeline.make_exec_tool() not implemented")

    def make_tool(self, spec, **kwargs):
        if 'class' in spec and spec['class'] == 'CommandLineTool':
            return self.make_exec_tool(spec, **kwargs)
        else:
            return cwltool.workflow.defaultMakeTool(spec, **kwargs)

    def add_thread(self, thread):
        self.threads.append(thread)

    def wait(self):
        for i in self.threads:
            i.join()

    def output_callback(self, out, status):
        if status == 'success':
            log.info('Job completed!')
        else:
            log.info('Job failed...')
        log.debug("job done", out, status)
        self.output = out

class PipelineJob(object):
    def __init__(self, spec, pipeline):
        self.spec = spec
        self.pipeline = pipeline
        self.running = False
        
    def find_docker_requirement(self):
        container=DEFAULT_IMAGE
        for i in self.spec.get("requirements", []) + self.spec.get("hints", []):
            if i.get("class", "NA") == "DockerRequirement":
                container = i.get("dockerPull", DEFAULT_IMAGE)

        return container

    def run(self, dry_run=False, pull_image=True, **kwargs):
        raise Exception("PipelineJob.run() not implemented")

