import requests
import functools
import os
from pathlib import Path
from requests.adapters import HTTPAdapter, Retry
import shutil
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s : %(levelname)s : %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)


class SemanticScholar:

    def __init__(self,
                 api_key: str,
                 download_path: str,
                 snapshot_date: str,
                 overwrite_snapshot: bool = False):

        self.api_key = api_key
        self.download_path = download_path
        self.snapshot_date = snapshot_date
        self.overwrite_snapshot = overwrite_snapshot

        if self.overwrite_snapshot:
            if Path(self.download_path).exists() and Path(self.download_path).is_dir():
                shutil.rmtree(self.download_path)

            os.makedirs(self.download_path, exist_ok=False)

        else:
            os.makedirs(self.download_path, exist_ok=True)

    @staticmethod
    def download(entity, download_path: str, file_prefix: str, start: int=1) -> None:

        retries = Retry(total=3, backoff_factor=1)

        adapter = HTTPAdapter(max_retries=retries)

        with requests.Session() as session:

            session.mount(prefix='https://', adapter=adapter)

            for n, file in enumerate(entity['files'], start=1):
                if n >= start:
                    logging.info(f'Download file {n} of {len(entity["files"])}.')
                    with session.get(file, stream=True) as response:
                        with open(f'{download_path}/{file_prefix}-part{n}.jsonl.gz', 'wb') as file:
                            response.raw.read = functools.partial(response.raw.read, decode_content=False)
                            shutil.copyfileobj(response.raw, file)
                    logging.info(f'Successfully download file {n} of {len(entity["files"])}.')

    def download_papers(self, start: int=1) -> None:

        papers_download_path = os.path.join(self.download_path, 'papers')

        os.makedirs(papers_download_path, exist_ok=not self.overwrite_snapshot)

        logging.info('Download: papers')

        papers = requests.get(
            url=f'https://api.semanticscholar.org/datasets/v1/release/{self.snapshot_date}/dataset/papers',
            headers={'x-api-key': self.api_key}).json()

        if papers.get('files'):
            self.download(papers, papers_download_path, file_prefix='papers', start=start)
        elif papers.get('error'):
            logging.info(papers.get('error'))
        else:
            logging.info('An error occurred while downloading.')

    def download_venues(self, start: int=1) -> None:

        venues_download_path = os.path.join(self.download_path, 'venues')

        os.makedirs(venues_download_path, exist_ok=not self.overwrite_snapshot)

        logging.info('Download: venues')

        venues = requests.get(
            url=f'https://api.semanticscholar.org/datasets/v1/release/{self.snapshot_date}/dataset/publication-venues',
            headers={'x-api-key': self.api_key}).json()

        if venues.get('files'):
            self.download(venues, venues_download_path, file_prefix='venues', start=start)
        elif venues.get('error'):
            logging.info(venues.get('error'))
        else:
            logging.info('An error occurred while downloading.')

    def download_abstracts(self, start: int=1) -> None:

        abstracts_download_path = os.path.join(self.download_path, 'abstracts')

        os.makedirs(abstracts_download_path, exist_ok=not self.overwrite_snapshot)

        logging.info('Download: abstracts')

        abstracts = requests.get(
            url=f'https://api.semanticscholar.org/datasets/v1/release/{self.snapshot_date}/dataset/abstracts',
            headers={'x-api-key': self.api_key}).json()

        if abstracts.get('files'):
            self.download(abstracts, abstracts_download_path, file_prefix='abstracts', start=start)
        elif abstracts.get('error'):
            logging.info(abstracts.get('error'))
        else:
            logging.info('An error occurred while downloading.')

    def download_citations(self, start: int=1) -> None:

        citations_download_path = os.path.join(self.download_path, 'citations')

        os.makedirs(citations_download_path, exist_ok=not self.overwrite_snapshot)

        logging.info('Download: citations')

        citations = requests.get(
            url=f'https://api.semanticscholar.org/datasets/v1/release/{self.snapshot_date}/dataset/citations',
            headers={'x-api-key': self.api_key}).json()

        if citations.get('files'):
            self.download(citations, citations_download_path, file_prefix='citations', start=start)
        elif citations.get('error'):
            logging.info(citations.get('error'))
        else:
            logging.info('An error occurred while downloading.')

if __name__ == '__main__':
    s2 = SemanticScholar(api_key=os.environ['s2_key'],
                         download_path='/scratch/users/haupka/semantic-scholar-snapshot',
                         snapshot_date='2025-02-25',
                         overwrite_snapshot=False)
    s2.download_papers()
    s2.download_venues()
    s2.download_abstracts()
    #s2.download_citations()
