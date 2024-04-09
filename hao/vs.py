# -*- coding: utf-8 -*-
import json
import logging
import os
import sys

from . import jsons, paths

LOGGER = logging.getLogger(__name__)


def run():
    argv = sys.argv
    n_args = len(argv)
    if n_args == 1:
        print('Usage: h-code {module.name}')
        return

    try:
        module_name = argv[1]
        path_launch_json = paths.get('.vscode/launch.json')
        if not os.path.exists(path_launch_json):
            data = {'version': '0.2.0', 'configurations': []}
        else:
            with open(path_launch_json) as f:
                data = json.load(f)
        data.get('configurations').append({
            "name": module_name,
            "type": "debugpy",
            "request": "launch",
            "program": "-m",
            "env": {},
            "console": "integratedTerminal",
            "args": [module_name],
            "justMyCode": False
        })
        with open(path_launch_json, 'w') as f:
            f.write(jsons.prettify(data))
    except Exception as e:
        LOGGER.exception(e)
