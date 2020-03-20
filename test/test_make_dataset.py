'''
import unittest
import os.path
import urllib.request as requests

from src.data.make_dataset import download_data

class TestMakeDataset(unittest.TestCase):
    def test_download_data(self):
        print(requests.get(url, headers=headers).status_code)
        self.assertTrue(os.path.exists('data/raw/2018-air-quality.csv'))

if __name__ == '__main__':
    unittest.main()
'''