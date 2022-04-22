import requests
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class fetcher():
    """ Helper to retrieve data from API """
    def __init__(self, baseUrl, username, password):
        self.baseUrl = baseUrl
        self.session = None
        self.retryPolicy = Retry(
            total=5,
            read=5,
            connect=5,
            backoff_factor=1,
            status_forcelist=(502, 504), # 500, 
        )
        self.token = self.login(username, password)
        self.initiateSession(self.token)

    def login(self, username, password):
        r = requests.get(self.baseUrl + 'tokensrv/v1/login', auth=requests.auth.HTTPBasicAuth(username, password))
        r.raise_for_status()
        data = r.json()
        return data.get('user', {}).get('token')
    
    def initiateSession(self, token):
        self.session = requests.Session()
        self.session.mount(self.baseUrl, HTTPAdapter(max_retries=self.retryPolicy))
        self.session.headers.update({'Authorization': f'Bearer {token}'})

class ladekast(fetcher):

    def __init__(self, baseUrl, username, password):
        self.loginUrl = 'tokensrv/v1/login'
        super().__init__(baseUrl, username, password)

        
    def retrieve(self, url):
        try:
            r = self.session.get(self.baseUrl + url, timeout=5)
            r.raise_for_status()
            data = r.json()
            logging.debug(f"Data retrieved for url {url}: " + str(len(data)))
            return data
                      
        except Exception as err:
            logging.exception('Fails: {}'.format(err.__class__.__name__))
            raise
        
        logging.debug("retrieved")
        
    def download(self, url, local_filename='', location=''):
        try:
            r = self.session.get(self.baseUrl + url, timeout=5*60, stream=True)
            r.raise_for_status()
            i = 0
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024): 
                    if chunk: # filter out keep-alive new chunks
                        i = i + 1
                        logging.debug(f"Downloading content in chunks: {i}")
                        f.write(chunk)
                        f.flush()
            return local_filename
                      
        except Exception as err:
            logging.exception('Fails: {}'.format(err.__class__.__name__))
            raise
            
        logging.debug("retrieved")
            
    def change(self, url, data):
        try:
            r = self.session.post(self.baseUrl + url, timeout=5, json=data)
            r.raise_for_status()
            data = r.json()
            logging.debug(f"Data retrieved for url {url}: " + str(len(data)))
            return data
                      
        except Exception as err:
            logging.exception('Fails: {}'.format(err.__class__.__name__))
            raise
        
        logging.debug("retrieved")
        