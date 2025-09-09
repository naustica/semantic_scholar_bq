import requests
import functools
import os
from pathlib import Path
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
                 snapshot_date: str):

        self.api_key = api_key
        self.download_path = download_path
        self.snapshot_date = snapshot_date

        if Path(download_path).exists() and Path(download_path).is_dir():
            shutil.rmtree(self.download_path)

        os.makedirs(download_path, exist_ok=False)


    def download_papers(self):

        papers_download_path = os.path.join(self.download_path, 'papers')

        os.makedirs(papers_download_path, exist_ok=False)

        papers = requests.get(url=f'http://api.semanticscholar.org/datasets/v1/release/{self.snapshot_date}/dataset/papers',
                              headers={'x-api-key': self.api_key}).json()

        logging.info('Download: papers')

        for n, file in enumerate(papers['files'], start=1):
            logging.info(f'Download file {n} of {len(papers["files"])}.')
            with requests.get(file, stream=True) as response:
                with open(f'{papers_download_path}/papers-part{n}.jsonl.gz', 'wb') as file:
                    response.raw.read = functools.partial(response.raw.read, decode_content=False)
                    shutil.copyfileobj(response.raw, file)
            logging.info(f'Successfully download file {n} of {len(papers["files"])}.')

    def download_venues(self):

        venues_download_path = os.path.join(self.download_path, 'venues')

        os.makedirs(venues_download_path, exist_ok=False)

        logging.info('Download: venues')

        venues = requests.get(url=f'http://api.semanticscholar.org/datasets/v1/release/{self.snapshot_date}/dataset/publication-venues',
                              headers={'x-api-key': self.api_key}).json()

        for n, file in enumerate(venues['files'], start=1):
            logging.info(f'Download file {n} of {len(venues["files"])}.')
            with requests.get(file, stream=True) as response:
                with open(f'{venues_download_path}/venues-part{n}.jsonl.gz', 'wb') as file:
                    response.raw.read = functools.partial(response.raw.read, decode_content=False)
                    shutil.copyfileobj(response.raw, file)
            logging.info(f'Successfully download file {n} of {len(venues["files"])}.')

    def download_abstracts(self):

        abstracts_download_path = os.path.join(self.download_path, 'abstracts')

        os.makedirs(abstracts_download_path, exist_ok=False)

        logging.info('Download: abstracts')

        abstracts = requests.get(url=f'http://api.semanticscholar.org/datasets/v1/release/{self.snapshot_date}/dataset/abstracts',
                              headers={'x-api-key': self.api_key}).json()

        for n, file in enumerate(abstracts['files'], start=1):
            logging.info(f'Download file {n} of {len(abstracts["files"])}.')
            with requests.get(file, stream=True) as response:
                with open(f'{abstracts_download_path}/abstracts-part{n}.jsonl.gz', 'wb') as file:
                    response.raw.read = functools.partial(response.raw.read, decode_content=False)
                    shutil.copyfileobj(response.raw, file)
            logging.info(f'Successfully download file {n} of {len(abstracts["files"])}.')

    def download_citations(self):

        citations_download_path = os.path.join(self.download_path, 'citations')

        os.makedirs(citations_download_path, exist_ok=False)

        logging.info('Download: citations')

        with requests.Session() as session:

            citations = session.get(
                url=f'http://api.semanticscholar.org/datasets/v1/release/{self.snapshot_date}/dataset/citations',
                headers={'x-api-key': self.api_key}).json()

            for n, file in enumerate(citations['files'], start=1):
                logging.info(f'Download file {n} of {len(citations["files"])}.')
                with session.get(file, stream=True) as response:
                    with open(f'{citations_download_path}/citations-part{n}.jsonl.gz', 'wb') as file:
                        response.raw.read = functools.partial(response.raw.read, decode_content=False)
                        shutil.copyfileobj(response.raw, file)
                logging.info(f'Successfully download file {n} of {len(citations["files"])}.')


if __name__ == '__main__':
    s2 = SemanticScholar(api_key=os.environ['s2_key'],
                         download_path='/scratch/users/haupka/semantic-scholar-snapshot',
                         snapshot_date='2025-02-25')
    s2.download_papers()
    s2.download_venues()
    s2.download_abstracts()
    #s2.download_citations()