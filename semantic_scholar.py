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

    def download_entity(self, entity: str, start: int=1) -> None:

        if entity not in ['papers', 'publication-venues', 'abstracts', 'citations']:
            raise ValueError("Entity must have one of the following values: \n"
                             "['papers', 'publication-venues', 'abstracts', 'citations']")

        entity_download_path = os.path.join(self.download_path, entity)

        os.makedirs(entity_download_path, exist_ok=not self.overwrite_snapshot)

        logging.info(f'Download: {entity}')

        entity_data = requests.get(
            url=f'https://api.semanticscholar.org/datasets/v1/release/{self.snapshot_date}/dataset/{entity}',
            headers={'x-api-key': self.api_key}).json()

        if entity_data.get('files'):
            self.download(entity_data, entity_download_path, file_prefix=entity, start=start)
        elif entity_data.get('error'):
            logging.info(entity_data.get('error'))
        else:
            logging.info('An error occurred while downloading.')

if __name__ == '__main__':
    s2 = SemanticScholar(api_key=os.environ['s2_key'],
                         download_path='/scratch/users/haupka/semantic-scholar-snapshot',
                         snapshot_date='2025-02-25',
                         overwrite_snapshot=False)

    s2.download_entity(entity='papers')
    s2.download_entity(entity='publication-venues')
    s2.download_entity(entity='abstracts')
    #s2.download_entity(entity='citations')
