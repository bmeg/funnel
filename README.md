# funnel

Trigger the Google Genomics Pipeline API with CWL

![FUNNEL](https://github.com/bmeg/funnel/blob/master/resources/funnel.jpg)

## Goal and Disclaimer

The goal of this project is to accept a CWL description of a workflow and use that to trigger jobs on the Google Genomics Pipeline API (GP). Beyond this, it endeavors to run jobs that do not depend on each other for output in parallel, making the most use of available resources.

The system is working for the simple test case included (test/hashsplitter-workflow.cwl), which reads an input file, hashes it three different ways, then merges the output of those into a single file. Right now inputs must be supplied from a google bucket (GB) and output goes to a GB, so uploading and downloading of local inputs/outputs must be done manually for now.

## Setup

A couple of things need to be installed for Funnel to run. 

First, the latest version of cwl-runner:

    sudo pip install --upgrade cwl-runner

Then, the google api python client:

    sudo pip install google-api-python-client

Then you need to set up gcloud:

    export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)"
    echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
    sudo apt-get update && sudo apt-get install google-cloud-sdk
    sudo pip install -U Sphinx
    sudo pip install gcloud

Add some auxiliary libraries:

    sudo pip install httplib2shim

Now you should be ready to go!

## Usage

If you just want to run some jobs locally and take advantage of the parallel execution of non-dependent tasks, it works much like the cwltool it is built on:

    python -m funnel.main test/hashsplitter-workflow.cwl --input README.md

If you want to run on GP, you must have a [GP enabled account](https://cloud.google.com/genomics/install-genomics-tools) and have gathered enough information to fill out the `gce_config.yaml` with your information:

    project-id: machine-generated-837
    service-account: SOMENUMBER-compute@developer.gserviceaccount.com
    bucket: your-bucket
    output-file: path/to/where/you/want/google/pipeline/to/put/your/output

Once this is supplied, you can call the same command line as before with the `--gce` argument pointing to the GP config: this triggers usage of the GP. In addition, you must provide any input as a GB address:

    python -m funnel.main --gce gce_config.yaml test/hashsplitter-workflow.cwl --input gs://hashsplitter/input/README.md

If your input is actually in that bucket and your GP config is correct, this will output something in the bucket location `gs://hashsplitter/output/unify`, along with whatever intermediate output was the result of previous steps.

Enjoy!
