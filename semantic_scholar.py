import requests
import urllib
import os
from pathlib import Path
import shutil


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

        print('Download of papers')

        for n, file in enumerate(papers['files'], start=1):
            print(f'Download file {n} of {len(papers["files"])}.')
            urllib.request.urlretrieve(file, f'{papers_download_path}/papers-part{n}.jsonl.gz')
            print(f'Successfully download file {n} of {len(papers["files"])}.')

    def download_venues(self):

        venues_download_path = os.path.join(self.download_path, 'venues')

        os.makedirs(venues_download_path, exist_ok=False)

        print('Download of venues')

        venues = requests.get(url=f'http://api.semanticscholar.org/datasets/v1/release/{self.snapshot_date}/dataset/publication-venues',
                              headers={'x-api-key': self.api_key}).json()

        for n, file in enumerate(venues['files'], start=1):
            print(f'Download file {n} of {len(venues["files"])}.')
            urllib.request.urlretrieve(file, f'{venues_download_path}/venues-part{n}.jsonl.gz')
            print(f'Successfully download file {n} of {len(venues["files"])}.')

    def download_abstracts(self):

        abstracts_download_path = os.path.join(self.download_path, 'abstracts')

        os.makedirs(abstracts_download_path, exist_ok=False)

        print('Download of abstracts')

        abstracts = requests.get(url=f'http://api.semanticscholar.org/datasets/v1/release/{self.snapshot_date}/dataset/abstracts',
                              headers={'x-api-key': self.api_key}).json()

        for n, file in enumerate(abstracts['files'], start=1):
            print(f'Download file {n} of {len(abstracts["files"])}.')
            urllib.request.urlretrieve(file, f'{abstracts_download_path}/venues-part{n}.jsonl.gz')
            print(f'Successfully download file {n} of {len(abstracts["files"])}.')


if __name__ == '__main__':
    s2 = SemanticScholar(api_key=os.environ['s2_key'],
                         download_path='/scratch/users/haupka/semantic-scholar-snapshot',
                         snapshot_date='2025-02-25')
    s2.download_papers()
    s2.download_venues()