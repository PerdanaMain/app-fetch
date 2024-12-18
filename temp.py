import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Union
import concurrent.futures
from itertools import islice
from config import Config
from model import Model

class AsyncHistoricalFetcher:
    def __init__(self):
        self.config = Config()
        self.logger = self.config.logger
        self.model = Model()
        self.timezone_offset = timedelta(hours=7)
        self.session = None
        self.batch_size = 500
        self.concurrent_batches = 4
        self.semaphore = asyncio.Semaphore(100)

    def validate_value(self, response: dict) -> Optional[Union[float, int]]:
        """
        Validasi response dan ekstrak nilai Value yang valid.
        
        Args:
            response (dict): Response dari API
            
        Returns:
            Optional[Union[float, int]]: Nilai Value jika valid, None jika tidak valid
        """
        try:
            if not isinstance(response, dict):
                self.logger.error(f"Invalid response format: {response}")
                return None
                
            value = response.get('Value')
            
            # Validasi nilai None atau empty
            if value is None:
                self.logger.error(f"Value is None in response: {response}")
                return None
                
            # Coba konversi ke float
            try:
                value = float(value)
            except (ValueError, TypeError):
                self.logger.error(f"Value is not a number: {value}")
                return None
                
            # Validasi untuk NaN atau Infinity
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                self.logger.error(f"Value is not a valid number: {value}")
                return None
                
            return value
            
        except Exception as e:
            self.logger.error(f"Error validating value: {str(e)}")
            return None

    async def create_session(self):
        """Membuat session aiohttp dengan autentikasi basic."""
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=30)
        auth = aiohttp.BasicAuth(
            login=self.config.PIWEB_API_USERNAME,
            password=self.config.PIWEB_API_PASSWORD
        )
        self.session = aiohttp.ClientSession(
            auth=auth,
            connector=connector,
            timeout=timeout
        )

    async def fetch_tag_data(self, tag: Tuple[str, str], timestamp: datetime) -> Optional[dict]:
        """Fetch data untuk satu tag dengan validasi nilai."""
        async with self.semaphore:
            if not self.session:
                await self.create_session()
                
            url = f"{self.config.PIWEB_API_URL}/streams/{tag[1]}/value?time={timestamp}"
            try:
                async with self.session.get(url, ssl=False) as response:
                    if response.status == 429:
                        await asyncio.sleep(1)
                        return await self.fetch_tag_data(tag, timestamp)
                        
                    data = await response.json()
                    validated_value = self.validate_value(data)
                    
                    if validated_value is not None:
                        return {
                            'tag_id': tag[0],
                            'timestamp': timestamp,
                            'value': validated_value
                        }
                    else:
                        self.logger.warning(f"Invalid value for tag {tag[1]} at {timestamp}")
                        return None
                        
            except Exception as e:
                self.logger.error(f"Error fetching tag {tag[1]}: {str(e)}")
                return None

    async def process_batch(self, tags: List[Tuple[str, str]], timestamp: datetime):
        """Proses satu batch tag dengan validasi."""
        tasks = [self.fetch_tag_data(tag, timestamp) for tag in tags]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_results = []
        for tag, result in zip(tags, results):
            if isinstance(result, Exception):
                self.logger.error(f"Failed to fetch {tag[1]}: {str(result)}")
            elif result is not None:  # Hanya proses hasil yang valid
                valid_results.append(result)
                print(f"Valid data for tag {tag[1]}: {result}")
                
        return valid_results

    def chunks(self, lst, n):
        """Membagi list menjadi chunks dengan ukuran n."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    async def fetch_data_for_timestamp(self, timestamp: datetime):
        """Fetch semua tag untuk satu timestamp dengan batching."""
        tags = self.model.get_all_tags()
        tag_batches = list(self.chunks(tags, self.batch_size))
        
        all_results = []
        for batch_group in self.chunks(tag_batches, self.concurrent_batches):
            tasks = [self.process_batch(batch, timestamp) for batch in batch_group]
            batch_results = await asyncio.gather(*tasks)
            for results in batch_results:
                all_results.extend(results)
                
        return all_results

    async def main(self):
        """Main loop untuk mengambil data historis."""
        try:
            start_date = datetime(2023, 7, 1)
            end_date = datetime.now() + self.timezone_offset
            
            self.logger.info(f"Mulai mengambil data dari {start_date} sampai {end_date}")
            
            await self.create_session()
            
            current_date = start_date
            while current_date <= end_date:
                self.logger.info(f"Processing data for {current_date}")
                results = await self.fetch_data_for_timestamp(current_date)
                self.logger.info(f"Processed {len(results)} valid records for {current_date}")
                current_date += timedelta(minutes=1)
                
        except Exception as e:
            self.logger.error(f"Error in main loop: {str(e)}")
        finally:
            if self.session:
                await self.session.close()

    def run(self):
        """Entry point untuk menjalankan fetcher."""
        asyncio.run(self.main())

if __name__ == "__main__":
    fetcher = AsyncHistoricalFetcher()
    fetcher.run()