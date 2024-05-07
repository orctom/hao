# -*- coding: utf-8 -*-
import ast
import logging
import os
import sys

from . import jsons, paths

LOGGER = logging.getLogger(__name__)


def run():
    argv = sys.argv
    n_args = len(argv)
    if n_args == 1:
        print('Usage: h-vs {module.name}')
        return

    try:
        module_name = argv[-1]
        path_launch_json = paths.get('.vscode/launch.json')
        if not os.path.exists(path_launch_json):
            data = {'version': '0.2.0', 'configurations': []}
            paths.make_parent_dirs(path_launch_json)
        else:
            with open(path_launch_json) as f:
                text = f.read()
                text = text.replace('null', 'None').replace('true', 'True').replace('false', 'False')
                data = ast.literal_eval(text)
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
