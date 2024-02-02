#!/bin/bash

# Virtual Environment Path
VENV_PATH="/home/shandytp/learn-luigi/pertemuan-14/live-class/venv/bin/activate"

# Activate Virtual Environment
source "$VENV_PATH"

# Set Python script
PYTHON_SCRIPT="/home/shandytp/learn-luigi/pertemuan-14/live-class/etl_pipeline.py"

# Run Python Script and insert log
python "$PYTHON_SCRIPT" >> /home/shandytp/learn-luigi/pertemuan-14/live-class/log/luigi_process.log 2>&1

# Luigi info simple log
dt=$(date '+%d/%m/%Y %H:%M:%S');
echo "Luigi started at ${dt}" >> /home/shandytp/learn-luigi/pertemuan-14/live-class/log/luigi_info.log