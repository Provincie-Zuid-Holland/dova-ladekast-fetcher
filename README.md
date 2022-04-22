# dova-ladekast-fetcher
Simple Python package to talk to DOVA Ladekast API.

# Goal
The goal of the package is to have an easy interface to use the API in Python.

Python package is developed within Provincie Zuid-Holland.
The "DOVA Ladekast API" is developed by DOVA (dova.nu).
To get access to the DOVA Ladekast please reach DOVA.


# Quick start
## Requirements
1. `pip install dova-ladekast-fetcher`

##  Sample code
```python
import getpass
import dova-ladekast-fetcher.api as dlf

# Initiate connection for further sessions
baseUrl = "dova-ladekast-url"
username = getpass.getpass(prompt='DOVA Ladekast username:')
password = getpass.getpass(prompt='DOVA Ladekast password:')
l = ladekast(baseUrl, username, password)

# Download specific dataset
dynamicDatasetUID = "GUID"          # <--- fill in GUID of dataset --->
filePath = "output_directory"       # <--- fill in path to store data --->
# Download the file
l.download(f'ladekast/v1/getdata/{dynamicDatasetUID}', filePath)
```

# Development
Package is hosted on GitHub. After each change increase version number and create a new Release on GitHub. The pipeline will trigger a release to PyPi (see status batch above).

## Collaborate?
Send a PR!

# Disclaimer
The developers of this package are not affiliated with DOVA.