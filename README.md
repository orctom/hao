# hao

configurations, logs and others.

## install

```bash
pip install hao
```

## precondition

The folder contained any of the following files (searched in this very order) will be treated as **project root path**.

- pyproject.toml
- requirements.txt
- setup.py
- LICENSE
- .idea
- .git
- .vscode

**If your project structure does NOT conform to this, it will not work as expected.**

## features

### config

It will try to load YAML config file from `conf` folder
```
.                               # project root
├── conf
│   ├── config-{env}.yml        # if `export env=abc`, will raise error if not found
│   ├── config-{hostname}.yml   # try to load this file, then the default `config.yml`
│   └── config.yml              # the default config file that should always exist
├── pyproject.toml              # or requirements.txt
├── .git
```

In following order:

```python
if os.environ.get("env") is not None:
    try_to_load(f'config-{env}.yml', fallback='config.yml')                   # echo $env
else:
    try_to_load(f'config-{socket.gethostname()}.yml', fallback='config.yml')  # echo hostname
```

Say you have the following content in your config file:
```yaml
# config.yml
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

Set the logger levels to filter logs

e.g.
```yaml
# config.yml
logging:
  __main__: DEBUG
  transformers: WARNING
  lightning: INFO
  pytorch_lightning: INFO
  elasticsearch: WARNING
  tests: DEBUG
  root: INFO                        # root level
```

Settings for logger:
```yaml
# config.yml
logger:
  format: "%(asctime)s %(levelname)-7s %(name)s:%(lineno)-4d - %(message)s"   # overwrite to change to other format
  handlers:
    TimedRotatingFileHandler:    # any Handlers in `logging` and `logging.handlers` with it's config
      when: d
      backupCount: 3
```

Example
```yaml
logger:
  format: "%(asctime)s %(levelname)-7s %(name)s:%(lineno)-4d - %(message)s"   # overwrite to change to other format
  handlers:
    stdout:
      format: "%(asctime)s %(levelname)-7s %(name)s:%(lineno)-4d - %(message)s"   # overwrite to change to other format
    file:
      format: "%(message)s"   # overwrite to change to other format
      handler: TimedRotatingFileHandler
      args:
        when: d
        backupCount: 3
        filename: test.log
    rolling-file:
      handler: TimedRotatingFileHandler
      args:
        when: d
        backupCount: 3
        filename: hello.log

logging:
  root: INFO
  torch.models: INFO
  __main__: DEBUG
  access:
    level: INFO
    handlers:
      - stdout
      - rolling-file
  test:
    level: INFO
    handlers:
      - file
```

Declare and user the logger

```python
import hao
LOGGER = hao.logs.get_logger(__name__)

LOGGER.debug('message')
LOGGER.info('message')
LOGGER.warnning('message')
LOGGER.error('message')
LOGGER.exception(err)
```

### namespaces

```python
import hao
from hao.namespaces import from_args, attr

@from_args
class ProcessConf(object):
    file_in = attr(str, required=True, help="file path to process")
    file_out = attr(str, required=True, help="file path to save")
    tokenizer = attr(str, required=True, choice=('wordpiece', 'bpe'))


from argparse import Namespace
from pytorch_lightning import Trainer
@from_args(adds=Trainer.add_argparse_args)
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

