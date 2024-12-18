from config import Config

class Model:
    def __init__(self):
        self.config = Config()
        self.logger = self.config.logger

    def get_all_tags(self):
        """Mengambil data dari database."""
        try:
            conn = self.config.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT id, web_id, name FROM dl_ms_tag")
            tags = cur.fetchall()

            return tags
        except Exception as e:
            self.logger.error(f"An exception occurred: {e}")