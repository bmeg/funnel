#!/usr/bin/env cwl-runner

cwlVersion: "draft-3"

class: CommandLineTool

description: "hash input through whirlpool"

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
      glob: whirlpool

stdout: whirlpool

baseCommand: ["openssl", "dgst"]

arguments: ["-sha512"]

