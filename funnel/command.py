import threading
import logging

from pipeline import Pipeline
import cwltool.job
import cwltool.draft2tool

log = logging.getLogger('funnel')

class CommandJob(cwltool.job.CommandLineJob):
    def __init__(self, spec):
        super(CommandJob, self).__init__()
        self.spec = spec

    def run(self, dry_run=False, pull_image=True, **kwargs):
        this = self

        def runnnn(this, kwargs):
            log.debug("Starting Thread")
            super(CommandJob, this).run(**kwargs)

        thread = threading.Thread(target=runnnn, args=(this, kwargs))
        thread.start()

class CommandTool(cwltool.draft2tool.CommandLineTool):
    def __init__(self, spec, **kwargs):
        super(cwltool.draft2tool.CommandLineTool, self).__init__(spec, **kwargs)
        self.spec = spec
        
    def makeJobRunner(self):
        return CommandJob(self.spec)

    def makePathMapper(self, reffiles, stagedir, **kwargs):
        useDocker = False
        for i in self.spec.get("requirements", []) + self.spec.get("hints", []):
            if i.get("class", "NA") == "DockerRequirement":
                useDocker = True
        # if useDocker:
        #     return cwltool.pathmapper.DockerPathMapper(reffiles, kwargs['basedir'])
        return cwltool.pathmapper.PathMapper(reffiles, stagedir, kwargs['basedir'])

class CommandPipeline(Pipeline):
    def __init__(self, config):
        super(CommandPipeline, self).__init__(config)

    def make_exec_tool(self, spec, **kwargs):
        return CommandTool(spec, **kwargs)

