import os
import sys
import json
import logging
import concurrent.futures

from tqdm import tqdm

from . import http

from xylose.scielodocument import Article


LOGGER = logging.getLogger(__name__)

XML_URL = "http://articlemeta.scielo.org/api/v1/article/?collection={col}&code={pid}&format={fmt}"
COLLECTIONS_URL = "http://articlemeta.scielo.org/api/v1/collection/identifiers/"
ARTICLE_IDENTIFIERS_URL = "http://articlemeta.scielo.org/api/v1/article/identifiers/?collection={col}&from={from_dt}&limit={limit}&offset={offset}"

INITIAL_DATE = "1900-01-01"
DEFAULT_FROM_DATE = (datetime.now() - timedelta(days=60)).isoformat()[:10]
DEFAULT_WORKING_DIR = os.path.join(os.path.expanduser("~"), ".scielo-dumps")


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


def splitted_lines(pids_file, fmt, working_dir, extension=".xml"):
    for line in pids_file:
        if len(line.strip()) == 0:
            continue
        collection, pid = line.strip().split()
        dest = os.path.join(
            working_dir, fmt, collection, pid[1:10], pid + extension)
        yield collection, pid, dest


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


def dump(workingdir, pids_file, pbar=dummy_tqdm, concurrency=2, fmt='json', extension='.json', overwrite=False, preservenull=True):
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
            LOGGER.debug('started a thread pool of size "%s"', executor._max_workers)
            future_to_file = {
                executor.submit(
                    download_doc,
                    XML_URL.format(col=collection, pid=pid, fmt=fmt),
                    dest,
                    pill,
                    overwrite=overwrite,
                    preserve_null=preservenull,
                ): dest
                for collection, pid, dest in progress_bar(
                    splitted_lines(pids_file, fmt, workingdir, extension)
                )
            }
            print("Done.", file=sys.stderr, flush=True)
            print(
                'Downloading files to "%s".' % workingdir,
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
            yield col, doc["code"]

        offset += limit


def new_pids(pid_filepath, from_date, collection=None):
    LOGGER.info('fetching documents published after "%s"', from_date)
    if collection:
        collections = [collection]
    else:
        collections = eligible_collections()
    pids = []
    for col in collections:
        for doc in iter_docs(col, from_date):
            pids.append(doc[0] + " " + doc[1])
    conteudo = "\n".join(pids)
    with open(pid_filepath, "w") as fp:
        fp.write(conteudo)


def dump_json(collection):
    from_date = INITIAL_DATE
    workdir = os.path.join(DEFAULT_WORKING_DIR, "json", collection)
    dumped_data_dir = os.path.join(workdir, "data")
    pids_filepath = os.path.join(workdir, "pids.txt")
    last_filepath = os.path.join(workdir, "lastdate.txt")

    if not os.path.dirname(dumped_data_dir):
        os.makedirs(dumped_data_dir)

    if os.path.isfile(last_filepath):
        with open(last_filepath, "r") as last_file:
            from_date = last_file.read()

    new_pids(pids_filepath, from_date, collection)

    with open(pids_filepath, "r") as pids_file:
        dump(
            dumped_data_dir, pids_file,
            pbar=dummy_tqdm, concurrency=2,
            fmt='json', extension='.json', overwrite=False, preservenull=True)

    with open(last_filepath, "w") as last_file:
        last_file.write(DEFAULT_FROM_DATE)
    return dumped_data_dir


def documents(json_path):
    for f in os.listdir(json_path):
        file_path = os.path.join(json_path, f)
        with open(file_path, "r") as fp:
            content = fp.read()
            document = Article(content)
            yield document
