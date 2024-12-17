import schedule  # type: ignore
import time
from config import Config

class Scheduler:
    def __init__(self):
        self.config = Config()
        self.logger = self.config.logger

    def task(self):
        try:
            conn = self.config.check_pi_connection()
            while True:
                if conn == False:
                  self.logger.error("Koneksi PI gagal")
                  time.sleep(300)
                else:
                  self.logger.info("Memulai task...")
                  break
            
            
            # Simulasi task
            time.sleep(1)
            self.logger.info("Task selesai")
            
        except Exception as e:
            self.logger.error(f"Task gagal: {str(e)}")

    def main(self):
        schedule.every().second.do(self.task)
        
        self.logger.info("Scheduler started")
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
                
            except KeyboardInterrupt:
                self.logger.info("Scheduler stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Error in main loop: {str(e)}")
                time.sleep(300)

if __name__ == "__main__":
    scheduler = Scheduler()
    scheduler.main()