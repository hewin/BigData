#!/bin/bash

source ~/.bashrc
BIN_DIR=$(cd `dirname $0`;pwd)

python ${BIN_DIR}/generate_table_conf.py ${BIN_DIR}/table.txt ${BIN_DIR}
