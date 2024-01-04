import os

from hao import paths

from . import logs
from .namespaces import attr, from_args

LOGGER = logs.get_logger(__name__)


@from_args
class Conf:
    model: str = attr(str, help='model name in huggingface')
    dataset: str = attr(str, help='datasaet name in huggingface')
    token: str = attr(str, env='hf_token', secret=True, help='access token in huggingface')
    include: str = attr(str, help='include files in downloading')
    exclude: str = attr(str, help='exclude files from downloading')
    save_to: str = attr(str, default='.', help='path to save')
    use_symlinks: bool = attr(bool, default=True, help='whether use symlinks')
    use_hf_transfer: bool = attr(bool, default=True, help='whether use hf-transfer')
    use_mirror: bool = attr(bool, default=True, help='whether use mirror')

    @property
    def token_option(self):
        return f"--token {self.token} " if self.token else ""

    @property
    def include_option(self):
        return f"--include {self.include} " if self.include else ""

    @property
    def exclude_option(self):
        return f"--exclude {self.exclude} " if self.exclude else ""

    @property
    def save_to_option(self):
        return f"--local-dir {paths.get(self.save_to, self.model.replace('/', '-'))} "

    @property
    def use_symlinks_option(self):
        return f"--local-dir-use-symlinks {self.use_symlinks} "


def import_huggingface_hub():
    try:
        import huggingface_hub
    except ImportError:
        LOGGER.info('Installing huggingface_hub')
        os.system("pip install -U huggingface_hub")


def import_hf_transfer():
    try:
        import hf_transfer
    except ImportError:
        LOGGER.info('Installing hf-transfer')
        os.system("pip install -U hf-transfer")

    os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"
    LOGGER.info(f"export HF_HUB_ENABLE_HF_TRANSFER={os.getenv('HF_HUB_ENABLE_HF_TRANSFER')}")


def setup_mirror():
    os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"
    LOGGER.info(f"export HF_ENDPOINT={os.getenv('HF_ENDPOINT')}")


def download_model(conf: Conf):
    download_shell = (
        "huggingface-cli download "
        f"{conf.token_option}"
        f"{conf.include_option}"
        f"{conf.exclude_option}"
        f"{conf.save_to_option}"
        f"{conf.model} "
        f"{conf.use_symlinks_option}"
        "--resume-download"
    )
    os.system(download_shell)


def download_dataset(conf: Conf):
    download_shell = (
        "huggingface-cli download "
        f"{conf.token_option}"
        f"{conf.include_option}"
        f"{conf.exclude_option}"
        f"{conf.save_to_option}"
        f"{conf.dataset} "
        f"{conf.use_symlinks_option}"
        "--resume-download --repo-type dataset"
    )
    os.system(download_shell)


def download():
    conf = Conf()
    LOGGER.info(conf)
    if not ((conf.model is None) ^ (conf.dataset is None)):
        LOGGER.warning('Expecting (only) one of "model" or "dataset" be specified.')
        return
    try:
        import_huggingface_hub()
        if conf.use_hf_transfer:
            import_hf_transfer()
        if conf.use_mirror:
            setup_mirror()
        if conf.save_to:
            paths.make_parent_dirs(paths.get(conf.save_to))

        if conf.model is not None:
            download_model(conf)
        elif conf.datasaet is not None:
            download_dataset(conf)

    except KeyboardInterrupt:
        print('[ctrl-c] stopped')
    except Exception as err:
        LOGGER.exception(err)


if __name__ == '__main__':
    download()
