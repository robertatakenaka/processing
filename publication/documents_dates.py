# coding: utf-8
"""
Este processamento gera uma tabulação de datas do artigo (publicação, submissão,
    aceite, entrada no scieloe atualização no scielo)
"""
import argparse
import logging
import codecs
import datetime
import csv
import utils
import choices

logger = logging.getLogger(__name__)


def _config_logging(logging_level='INFO', logging_file=None):

    allowed_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.setLevel(allowed_levels.get(logging_level, 'INFO'))

    if logging_file:
        hl = logging.FileHandler(logging_file, mode='a')
    else:
        hl = logging.StreamHandler()

    hl.setFormatter(formatter)
    hl.setLevel(allowed_levels.get(logging_level, 'INFO'))

    logger.addHandler(hl)

    return logger


class NewDumper(object):

    def __init__(self, collection, issns=None, output_file=None):
        self.csv_file = output_file or "documents_data.csv"
        self._ratchet = utils.ratchet_server()
        self._articlemeta = utils.articlemeta_server()
        self.collection = collection
        self.issns = issns

    @property
    def header(self):
        return [
            "extraction date",
            "study unit",
            "collection",
            "ISSN SciELO",
            "ISSN's",
            "title at SciELO",
            "title thematic areas",
            "title is agricultural sciences",
            "title is applied social sciences",
            "title is biological sciences",
            "title is engineering",
            "title is exact and earth sciences",
            "title is health sciences",
            "title is human sciences",
            "title is linguistics, letters and arts",
            "title is multidisciplinary",
            "title current status",
            "document publishing ID (PID SciELO)",
            "document publishing year",
            "document type",
            "document is citable",
            "document submitted at",
            "document submitted at year",
            "document submitted at month",
            "document submitted at day",
            "document accepted at",
            "document accepted at year",
            "document accepted at month",
            "document accepted at day",
            "document reviewed at",
            "document reviewed at year",
            "document reviewed at month",
            "document reviewed at day",
            "document published as ahead of print at",
            "document published as ahead of print at year",
            "document published as ahead of print at month",
            "document published as ahead of print at day",
            "document published at",
            "document published at year",
            "document published at month",
            "document published at day",
            "document published in SciELO at",
            "document published in SciELO at year",
            "document published in SciELO at month",
            "document published in SciELO at day",
            "document updated in SciELO at",
            "document updated in SciELO at year",
            "document updated in SciELO at month",
            "document updated in SciELO at day"
        ]

    def get_row_data(self, data):
        SUBJ_AREAS = [
            i.lower()
            for i in choices.THEMATIC_AREAS or []]
        J_SUBJ_AREAS = [
            i.lower()
            for i in data.journal.subject_areas or []]

        def get_flag(self, condition):
            return "1" if condition else "0"

        row = {}
        row["extraction date"] = datetime.datetime.now().isoformat()[0:10]
        row["study unit"] = u'document'
        row["collection"] = data.collection_acronym
        row["ISSN SciELO"] = data.journal.scielo_issn
        issns = [
            issn
            for issn in [data.journal.print_issn, data.journal.electronic_issn]
            if issn
        ]
        row["ISSN's"] = u';'.join(issns)
        row["title at SciELO"] = data.journal.title
        row["title thematic areas"] = u';'.join(
            data.journal.subject_areas or [])

        for area in SUBJ_AREAS:
            row["title is {}".format(area)] = get_flag(area in J_SUBJ_AREAS)
        row["title is multidisciplinary"] = get_flag(
            len(data.journal.subject_areas or []) > 2)
        row["title current status"] = data.journal.current_status
        row["document publishing ID (PID SciELO)"] = data.publisher_id
        row["document publishing year"] = data.publication_date[0:4]
        row["document type"] = data.document_type
        row["document is citable"] = get_flag(
            data.document_type.lower() in choices.CITABLE_DOCUMENT_TYPES)

        dates = [
            ("document submitted at", data.receive_date),
            ("document accepted at", data.acceptance_date),
            ("document reviewed at", data.review_date),
            ("document published as ahead of print at", data.ahead_publication_date),
            ("document published at", data.publication_date),
            ("document published in SciELO at", data.creation_date),
            ("document updated in SciELO at", data.update_date),
        ]
        for label, data_date in dates:
            row[label] = data_date or ''
            splitted_date = utils.split_date(row[label])
            row[label + " year"] = splitted_date[0]
            row[label + " month"] = splitted_date[1]
            row[label + " day"] = splitted_date[2]
        return row

    def create_csv_file(self, rows):
        with open(self.csv_file, 'w', newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.header)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    @property
    def rows(self):
        for issn in self.issns or []:
            for document in self._articlemeta.documents(
                    collection=self.collection, issn=issn):
                logger.debug('Reading document: %s' % document.publisher_id)
                yield self.get_row_data(document)

    def run(self):
        self.create_csv_file(self.rows)
        logger.info('Export finished')


def main():

    parser = argparse.ArgumentParser(
        description='Dump article dates'
    )

    parser.add_argument(
        'issns',
        nargs='*',
        help='ISSN\'s separated by spaces'
    )

    parser.add_argument(
        '--collection',
        '-c',
        help='Collection Acronym'
    )

    parser.add_argument(
        '--output_file',
        '-r',
        help='File to receive the dumped data'
    )

    parser.add_argument(
        '--logging_file',
        '-o',
        help='Full path to the log file'
    )

    parser.add_argument(
        '--logging_level',
        '-l',
        default='DEBUG',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logggin level'
    )

    args = parser.parse_args()
    _config_logging(args.logging_level, args.logging_file)
    logger.info('Dumping data for: %s' % args.collection)

    issns = None
    if len(args.issns) > 0:
        issns = utils.ckeck_given_issns(args.issns)

    dumper = NewDumper(args.collection, issns, args.output_file)

    dumper.run()
