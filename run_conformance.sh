#!/bin/bash

BDIR="$(cd `dirname $0`; pwd)"

if [ ! -e common-workflow-language ]; then
  git clone https://github.com/common-workflow-language/common-workflow-language.git
fi

if [ ! -e venv ]; then
  virtualenv venv
	venv/bin/pip install cwltool supervisor
fi

if [ ! -e venv ]; then
  git clone --recursive https://github.com/bmeg/task-execution-server.git
  make depends
fi

if [ ! -e var/storage ]; then
  mkdir -p var/storage
fi 

pushd task-execution-server
make
popd

source venv/bin/activate

supervisord
sleep 2

pushd common-workflow-language
./run_test.sh RUNNER=$BDIR/test/funnel-local-tes DRAFT=v1.0
popd

supervisorctl shutdown