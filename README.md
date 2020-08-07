# hao

configurations, logs and others.

## install

```bash
pip install hao
```

## features

**Precondition**: The folder contained any of the following files (searched in this very order) will be treated as **project root path**.

- requirements.txt
- VERSION
- conf
- setup.py
- .idea
- .git

If your project structure does NOT conform to this, it will not work as expected.

### config

It will try to load YAML config file from folder: `${project_root_path}/conf/`, by following order:

```python
if os.environ.get("env") is not None:
    try_to_load(f'config-{env}.yml', fallback='config.yml')                   # echo $env
else:
    try_to_load(f'config-{socket.gethostname()}.yml', fallback='config.yml')  # echo hostname
```

Say you have the following content in your config file:
```yaml
es:
  default:
    host: 172.23.3.3
    port: 9200
    indices:
      - news
      - papers
```

The get the configured values in your code:
```python
import hao
es_host = hao.config.get('es.default.host')          # str
es_port = hao.config.get('es.default.port')          # int
indices = hao.config.get('es.default.indices')       # list
...
```

### logs

Set the logger level in `config.yml`

e.g.
```yaml
logging:
  __main__: DEBUG
  transformers: WARNING
  lightning: INFO
  pytorch_lightning: INFO
  elasticsearch: WARNING
  tests: DEBUG
  root: INFO                        # root level
```

If you want to change the log format:
```yaml
logger:
  format: "%(asctime)s %(levelname)-7s %(name)s:%(lineno)-4d - %(message)s"
```

Declear your logger

``python
import hao
LOGGER = hao.logs.get_logger(__name__)
```

### namespaces

```python
from spanner.namespaces import from_args, attr

@from_args
#@from_args(adds=Trainer.add_argparse_args)
class TrainConf(Namespace):
    root_path_checkpoints = attr(str, default=hao.paths.get_path('data/checkpoints/'))
    dataset_train = attr(str, default='train.txt')
    dataset_val = attr(str, default='val.txt')
    dataset_test = attr(str, default='test.txt')
    batch_size = attr(int, default=128, key='train.batch_size')                          # key means try to load from config.yml by the key
    task = attr(str, choices=('ner', 'nmt'), default='ner')
    seed = attr(int)
    epochs = attr(int, default=5)
```

Where `attr` is a wrapper for `argpars.add_argument()`

Usage 1: overwrite the default value from command line

```shell
python -m your_module --task=nmt
```

Usage 2: overwrite the default value from constructor
```python
train_conf = TrainConf(task='nmt')
```

Value lookup order:

- command line
- constructor
- config yml if `key` specified in `attr`
- `default` if specified in `attr`

