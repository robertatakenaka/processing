import os
import sys
import json
import logging
import concurrent.futures

from tqdm import tqdm

from . import http

LOGGER = logging.getLogger(__name__)

XML_URL = "http://articlemeta.scielo.org/api/v1/article/?collection={col}&code={pid}&format={fmt}"
COLLECTIONS_URL = "http://articlemeta.scielo.org/api/v1/collection/identifiers/"
ARTICLE_IDENTIFIERS_URL = "http://articlemeta.scielo.org/api/v1/article/identifiers/?collection={col}&from={from_dt}&limit={limit}&offset={offset}"


class PoisonPill:
    """Sinaliza para as threads que devem abortar a execução da rotina e
    retornar imediatamente.
    """

    def __init__(self):
        self.poisoned = False


def download_doc(
    collection, pid, dest, pill, fmt, overwrite=False, preserve_null=False
):
    if pill.poisoned:
        return
    if not overwrite and os.path.exists(dest):
        LOGGER.info('file "%s" already exists. skipping.', dest)
        return
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    content = http.get(XML_URL.format(col=collection, pid=pid, fmt=fmt))
    # o ArticleMeta retorna a string b'null' em resposta a requisições para
    # pids inexistentes.
    if not preserve_null and len(content) == 4:
        LOGGER.info(
            'content got from "%s" is b"null". skipping.',
            XML_URL.format(col=collection, pid=pid, fmt=fmt),
        )
        return
    with open(dest, "wb") as dest_file:
        dest_file.write(content)


def splitted_lines(pids_file, fmt, working_dir):
    for line in pids_file:
        if len(line.strip()) == 0:
            continue
        collection, pid = line.strip().split()
        dest = os.path.join(working_dir, fmt, collection, pid[1:10], pid + ".xml")
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


def dump(args):
    if args.pbar:
        progress_bar = tqdm
    else:
        progress_bar = dummy_tqdm

    pill = PoisonPill()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.concurrency
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
                    collection,
                    pid,
                    dest,
                    pill,
                    args.format,
                    overwrite=args.overwrite,
                    preserve_null=args.preservenull,
                ): dest
                for collection, pid, dest in progress_bar(
                    splitted_lines(args.pids_file, args.format, args.workingdir)
                )
            }
            print("Done.", file=sys.stderr, flush=True)
            print(
                'Downloading files to "%s".' % args.workingdir,
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


def new_pids(args):
    LOGGER.info('fetching documents published after "%s"', args.from_date)
    for col in eligible_collections():
        for doc in iter_docs(col, args.from_date):
            args.output.write(doc[0] + " " + doc[1] + "\n")

