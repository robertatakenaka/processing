# coding: utf-8
"""
Este processamento gera uma tabulação de datas do artigo (publicação, submissão,
    aceite, entrada no scieloe atualização no scielo)
"""
import argparse
import logging
import codecs
import datetime

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


class Dumper(object):

    def __init__(self, collection, issns=None, output_file=None):

        self._ratchet = utils.ratchet_server()
        self._articlemeta = utils.articlemeta_server()
        self.collection = collection
        self.issns = issns
        self.output_file = codecs.open(output_file, 'w', encoding='utf-8') if output_file else output_file
        header = []
        header.append(u"extraction date")
        header.append(u"study unit")
        header.append(u"collection")
        header.append(u"ISSN SciELO")
        header.append(u"ISSN\'s")
        header.append(u"title at SciELO")
        header.append(u"title thematic areas")
        for area in choices.THEMATIC_AREAS:
            header.append(u"title is %s" % area.lower())
        header.append(u"title is multidisciplinary")
        header.append(u"title current status")
        header.append(u"document publishing ID (PID SciELO)")
        header.append(u"document publishing year")
        header.append(u"document type")
        header.append(u"document is citable")
        header.append(u"document submitted at")
        header.append(u"document submitted at year")
        header.append(u"document submitted at month")
        header.append(u"document submitted at day")
        header.append(u"document accepted at")
        header.append(u"document accepted at year")
        header.append(u"document accepted at month")
        header.append(u"document accepted at day")
        header.append(u"document reviewed at")
        header.append(u"document reviewed at year")
        header.append(u"document reviewed at month")
        header.append(u"document reviewed at day")
        header.append(u"document published as ahead of print at")
        header.append(u"document published as ahead of print at year")
        header.append(u"document published as ahead of print at month")
        header.append(u"document published as ahead of print at day")
        header.append(u"document published at")
        header.append(u"document published at year")
        header.append(u"document published at month")
        header.append(u"document published at day")
        header.append(u"document published in SciELO at")
        header.append(u"document published in SciELO at year")
        header.append(u"document published in SciELO at month")
        header.append(u"document published in SciELO at day")
        header.append(u"document updated in SciELO at")
        header.append(u"document updated in SciELO at year")
        header.append(u"document updated in SciELO at month")
        header.append(u"document updated in SciELO at day")

        self.write(u','.join([u'"%s"' % i.replace(u'"', u'""') for i in header]))

    def write(self, line):
        if not self.output_file:
            print(line.encode('utf-8'))
        else:
            self.output_file.write('%s\r\n' % line)

    def run(self):
        for item in self.items():
            self.write(item)
        logger.info('Export finished')

    def items(self):

        if not self.issns:
            self.issns = [None]

        for issn in self.issns:
            for data in self._articlemeta.documents(collection=self.collection, issn=issn):
                logger.debug('Reading document: %s' % data.publisher_id)
                yield self.fmt_csv(data)

    def fmt_csv(self, data):
        issns = []
        if data.journal.print_issn:
            issns.append(data.journal.print_issn)
        if data.journal.electronic_issn:
            issns.append(data.journal.electronic_issn)

        line = []
        line.append(datetime.datetime.now().isoformat()[0:10])
        line.append(u'document')
        line.append(data.collection_acronym)
        line.append(data.journal.scielo_issn)
        line.append(u';'.join(issns))
        line.append(data.journal.title)
        line.append(u';'.join(data.journal.subject_areas or []))
        for area in choices.THEMATIC_AREAS:
            if area.lower() in [i.lower() for i in data.journal.subject_areas or []]:
                line.append(u'1')
            else:
                line.append(u'0')
        line.append('1' if len(data.journal.subject_areas or []) > 2 else '0')
        line.append(data.journal.current_status)
        line.append(data.publisher_id)
        line.append(data.publication_date[0:4])
        line.append(data.document_type)
        line.append(u'1' if data.document_type.lower() in choices.CITABLE_DOCUMENT_TYPES else '0')
        line.append(data.receive_date or '')
        receive_splited = utils.split_date(data.receive_date or '')
        line.append(receive_splited[0])  # year
        line.append(receive_splited[1])  # month
        line.append(receive_splited[2])  # day
        line.append(data.acceptance_date or '')
        acceptance_splited = utils.split_date(data.acceptance_date or '')
        line.append(acceptance_splited[0])  # year
        line.append(acceptance_splited[1])  # month
        line.append(acceptance_splited[2])  # day
        line.append(data.review_date or '')
        review_splited = utils.split_date(data.review_date or '')
        line.append(review_splited[0])  # year
        line.append(review_splited[1])  # month
        line.append(review_splited[2])  # day
        line.append(data.ahead_publication_date or '')
        ahead_publication_date_splited = utils.split_date(data.ahead_publication_date or '')
        line.append(ahead_publication_date_splited[0])  # year
        line.append(ahead_publication_date_splited[1])  # month
        line.append(ahead_publication_date_splited[2])  # day
        line.append(data.publication_date or '')
        publication_splited = utils.split_date(data.publication_date or '')
        line.append(publication_splited[0])  # year
        line.append(publication_splited[1])  # month
        line.append(publication_splited[2])  # day
        line.append(data.creation_date or '')
        creation_splited = utils.split_date(data.creation_date or '')
        line.append(creation_splited[0])  # year
        line.append(creation_splited[1])  # month
        line.append(creation_splited[2])  # day
        line.append(data.update_date or '')
        update_splited = utils.split_date(data.update_date or '')
        line.append(update_splited[0])  # year
        line.append(update_splited[1])  # month
        line.append(update_splited[2])  # day
        joined_line = ','.join(['"%s"' % i.replace('"', '""') for i in line])

        return joined_line


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

    dumper = Dumper(args.collection, issns, args.output_file)

    dumper.run()
