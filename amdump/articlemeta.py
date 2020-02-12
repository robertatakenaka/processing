import os
import sys
import json
import logging
import concurrent.futures
from datetime import datetime, timedelta

from tqdm import tqdm

from . import http

from xylose.scielodocument import Article


LOGGER = logging.getLogger(__name__)

DOC_DATA_URL = "http://articlemeta.scielo.org/api/v1/article/?collection={col}&code={pid}&format={fmt}"
COLLECTIONS_URL = "http://articlemeta.scielo.org/api/v1/collection/identifiers/"
ARTICLE_IDENTIFIERS_URL = "http://articlemeta.scielo.org/api/v1/article/identifiers/?collection={col}&from={from_dt}&limit={limit}&offset={offset}"

FIRST_PUBLICATION_DATE = "1900-01-01"
DEFAULT_WORKING_DIR = os.path.join(os.path.expanduser("~"), "scielo-dumps")


def default_resume_date(from_date=datetime.now(), days=20):
    return (from_date - timedelta(days=days)).isoformat()[:10]


class PoisonPill:
    """Sinaliza para as threads que devem abortar a execução da rotina e
    retornar imediatamente.
    """

    def __init__(self):
        self.poisoned = False


def download_doc(
    url, dest, pill, overwrite=False, preserve_null=False
):
    if pill.poisoned:
        return
    if not overwrite and os.path.exists(dest):
        LOGGER.info('file "%s" already exists. skipping.', dest)
        return
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    content = http.get(url)
    # o ArticleMeta retorna a string b'null' em resposta a requisições para
    # pids inexistentes.
    if not preserve_null and len(content) == 4:
        LOGGER.info(
            'content got from "%s" is b"null". skipping.',
            url,
        )
        return
    with open(dest, "wb") as dest_file:
        dest_file.write(content)


def get_pid_and_dest(pids_file, collection, working_dir, extension=".xml"):
    for pid in pids_file:
        pid = pid.strip()
        if not pid:
            continue
        dest = os.path.join(working_dir, pid + extension)
        yield pid, dest


class dummy_tqdm:
    """Provê a interface do `tqdm` mas sem qualquer comportamento. É utilizado
    para suprimir a exibição da barra de progresso.
    """

    def __init__(self, iterable=None, total=None):
        self._it = iterable

    def __iter__(self):
        return self._it

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        return

    def update(self, num):
        return


def dump(collection, dest_dir, pids_file, progress_bar=dummy_tqdm, concurrency=2,
         fmt='json', extension='.json', overwrite=False, preservenull=True):
    pill = PoisonPill()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=concurrency
    ) as executor:
        print(
            "Reading the contents of pids_file. This may take a while.",
            file=sys.stderr,
            flush=True,
        )
        try:
            LOGGER.debug(
                'started a thread pool of size "%s"', executor._max_workers)
            future_to_file = {
                executor.submit(
                    download_doc,
                    DOC_DATA_URL.format(col=collection, pid=pid, fmt=fmt),
                    dest,
                    pill,
                    overwrite=overwrite,
                    preserve_null=preservenull,
                ): dest
                for pid, dest in progress_bar(
                    get_pid_and_dest(
                        pids_file, collection, dest_dir, extension)
                )
            }
            print("Done.", file=sys.stderr, flush=True)
            print(
                'Downloading files to "%s".' % dest_dir,
                file=sys.stderr,
                flush=True,
            )
            with progress_bar(total=len(future_to_file)) as pbar:
                for future in concurrent.futures.as_completed(future_to_file):
                    dest_file = future_to_file[future]
                    pbar.update(1)
                    try:
                        _ = future.result()
                    except KeyboardInterrupt:
                        raise
                    except Exception as exc:
                        LOGGER.exception('could not download "%s"', dest_file)
        except KeyboardInterrupt:
            LOGGER.info("terminating all pending tasks")
            pill.poisoned = True
            try:
                for future in future_to_file.keys():
                    future.cancel()
            except NameError:
                # when ctrl+c is hit before future_to_file is defined.
                pass
            raise


def eligible_collections():
    content = json.loads(http.get(COLLECTIONS_URL))
    eligibles = [
        c["code"]
        for c in content
        if c.get("status") in ["certified", "diffusion"] and c.get("is_active") == True
    ]

    return eligibles


def iter_docs(col, from_dt):
    limit = 500
    offset = 0

    while True:
        content = json.loads(
            http.get(
                ARTICLE_IDENTIFIERS_URL.format(
                    col=col, from_dt=from_dt, limit=limit, offset=offset
                )
            )
        )
        if not len(content.get("objects", [])):
            return
        for doc in content.get("objects", []):
            yield doc["code"], doc["processing_date"]

        offset += limit


class Dumper:

    def __init__(self, collection, from_date,
                 workdir=None,
                 pbar=dummy_tqdm, concurrency=2,
                 fmt='json', extension='.json',
                 overwrite=False, preservenull=True):
        self.workdir = workdir or DEFAULT_WORKING_DIR
        self.collection = collection
        self.default_from_date = from_date
        self.pbar = pbar
        self.concurrency = concurrency
        self.overwrite = overwrite
        self.preservenull = preservenull
        self._dates = []
        self._new_pids = []

    def dump_json(self):
        with open(self.pids_filepath, "r") as pids_file:
            dump(
                self.collection, self.json_files_path, pids_file,
                progress_bar=self.pbar, concurrency=self.concurrency,
                fmt='json', extension='.json',
                overwrite=self.overwrite, preservenull=self.preservenull)

        with open(self.last_filepath, "w") as last_file:
            last_file.write(self._dates[-1])

    @property
    def json_workdir(self):
        path = os.path.join(self.workdir, "json", self.collection)
        if not os.path.isdir(path):
            os.makedirs(path)
        return path

    @property
    def json_files_path(self):
        path = os.path.join(self.json_workdir, "data")
        if not os.path.isdir(path):
            os.makedirs(path)
        return path

    @property
    def pids_filepath(self):
        name = "pids_{}_{}.txt".format(
            self.from_date, datetime.now().isoformat()[:10])
        return os.path.join(self.json_workdir, name)

    @property
    def last_filepath(self):
        return os.path.join(self.json_workdir, "lastdate.txt")

    @property
    def dates_filepath(self):
        return os.path.join(self.json_workdir, "dates.txt")

    @property
    def from_date(self):
        if os.path.isfile(self.last_filepath):
            with open(self.last_filepath, "r") as last_file:
                return last_file.read()
        return self.default_from_date

    def _download_pids(self):
        LOGGER.info('fetching PIDs published after "%s"', self.from_date)
        for doc in iter_docs(self.collection, self.from_date):
            self._dates.append(doc[1])
            yield doc[0].strip()

    def download_pids(self):
        with open(self.pids_filepath, "w") as fp:
            fp.write("\n".join(self._download_pids()))
        with open(self.dates_filepath, "w") as fp:
            fp.write("\n".join(self._dates))

    @property
    def documents(self):
        for f in os.listdir(self.json_files_path):
            file_path = os.path.join(self.json_files_path, f)
            with open(file_path, "r") as fp:
                content = fp.read()
                document = Article(json.loads(content))
                yield document
