#!/usr/bin/env cwl-runner

cwlVersion: "draft-3"

class: CommandLineTool

description: "hash input through md5"

hints:
  - class: DockerRequirement
    dockerPull: bmeg/openssl

inputs:
  - id: input
    type: File
    description: "original content"

outputs:
  - id: output
    type: File
    outputBinding:
      glob: md5

stdout: md5

baseCommand: ["openssl", "dgst"]

arguments: ["-md5"]

