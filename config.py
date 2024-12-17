# config.py
import os
import logging
import psycopg2  # type: ignore
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime

import test
from dotenv import load_dotenv  # type: ignore

load_dotenv()

class Config:
    def __init__(self):
        # API PIWeb Configuration
        self.PIWEB_API_URL = os.getenv("PI_SERVER_ENDPOINT")
        self.PIWEB_API_USERNAME = os.getenv("PI_SERVER_USERNAME")
        self.PIWEB_API_PASSWORD = os.getenv("PI_SERVER_PASSWORD")
        
        # Setup logger saat inisialisasi
        self.logger = self._setup_logger()  # Menggunakan underscore untuk menandai private method

    def get_connection(self):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                port=os.getenv("DB_PORT")
            )
            self.logger.info("Database connection successful")
            return conn
        except Exception as e:
            self.logger.error(f"Error connecting to the database: {e}")
            return None

    def check_pi_connection(self):
      host = self.PIWEB_API_URL
      username = self.PIWEB_API_USERNAME
      password = self.PIWEB_API_PASSWORD
      
      try:
        test_conn = requests.get(
          host,
          auth=HTTPBasicAuth(
            username=username,
            password=password,
          ),
          verify=False,
          timeout=10,
        )
        if test_conn.status_code == 200:
          self.logger.info(f"PI Connection successful, status code: {test_conn.status_code}")
          return True
        else:
          self.logger.error(f"Received unexpected from PI, status code: {test_conn.status_code}")
          return False
        
      except Exception as e:
        self.logger.error(f"Error connecting to the PI Server: {e}")
        return None
    
    def _setup_logger(self):  # Tambahkan self sebagai parameter
        # Buat direktori logs jika belum ada
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # Format logging
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        
        # Buat logger baru dengan nama class
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # Hapus handler yang ada untuk menghindari duplicate
        if logger.hasHandlers():
            logger.handlers.clear()
        
        # Buat handler untuk file
        file_handler = logging.FileHandler(
            f'logs/scheduler_{datetime.now().strftime("%Y%m%d")}.log'
        )
        file_handler.setFormatter(logging.Formatter(log_format))
        
        # Buat handler untuk console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_format))
        
        # Tambahkan handlers ke logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger