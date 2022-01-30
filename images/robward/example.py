#!/usr/bin/env python
"""Entry point to import places and people csv files into database tables"""
from dataload import DataLoad

obj = DataLoad()
obj.ingest()
