import asyncio
import aiohttp # type: ignore
import schedule  # type: ignore
import time
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from config import Config
from model import Model

class Scheduler:
    def __init__(self):
        self.config = Config()
        self.logger = self.config.logger
        self.model = Model()
        # Tetapkan zona waktu Indonesia sekali di constructor
        self.timezone_offset = timedelta(hours=7)

    def task(self):
        """Task utama yang dijalankan secara terjadwal."""
        try:
            start_date = datetime(2024, 12, 17)            
            current_date = datetime.now() + self.timezone_offset
            
            while start_date <= current_date:
                print(f"Fetching data for {start_date}")
                
                username = self.config.PIWEB_API_USERNAME
                password = self.config.PIWEB_API_PASSWORD
                host = self.config.PIWEB_API_URL
                
                tags = self.model.get_all_tags()
                for tag in tags:
                    url = f"{host}/streams/{tag[1]}/value?time={start_date}"
                    try:
                        response = requests.get(
                            url,
                            auth=HTTPBasicAuth(username, password),
                            verify=False
                        ).json()
                        print(response)
                    except requests.exceptions.SSLError as e:
                        self.logger.error(f"SSL Error: {e}")
                    except Exception as e:
                        self.logger.error(f"Error: {e}")
                
                start_date += timedelta(minutes=1)
            
            self.logger.info("Task selesai")
            time.sleep(10)
            
        except Exception as e:
            self.logger.error(f"Task gagal: {str(e)}")
            # Tambahkan raise untuk debug jika perlu
            # raise

    def wait_for_pi_connection(self) -> bool:
        """Menunggu sampai koneksi PI tersedia."""
        while True:
            if self.config.check_pi_connection():
                self.logger.info("Koneksi PI berhasil")
                return True
            self.logger.error("Koneksi PI gagal, mencoba lagi dalam 5 menit...")
            time.sleep(300)  # 5 menit

    def main(self):
        """Main loop untuk menjalankan scheduler."""
        schedule.every().second.do(self.task)
        
        while True:
          try:
              # Pastikan koneksi PI tersedia sebelum mulai
              self.wait_for_pi_connection()
              self.logger.info("Scheduler started")

              while True:
                  schedule.run_pending()
                  time.sleep(1)

          except KeyboardInterrupt:
              self.logger.info("Scheduler stopped by user")
              break
          except Exception as e:
              self.logger.error(f"Error in main loop: {str(e)}")
              # Tunggu 5 menit sebelum restart
              time.sleep(300)
          except Exception as e:
              scheduler.logger.error(f"Scheduler crashed: {str(e)}")
              time.sleep(300)  # Tunggu 5 menit sebelum restart

if __name__ == "__main__":
    scheduler = Scheduler()
    scheduler.main()
    
    