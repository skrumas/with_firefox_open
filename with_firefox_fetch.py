#bing search and fetch
import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import os
import logging
import random
from tqdm.asyncio import tqdm_asyncio
import nest_asyncio
import time
import re
from urllib.parse import quote_plus

nest_asyncio.apply()

# konf.
BASE_DIR = "/workspaces/with_firefox_open/"
INPUT_EXCEL = "/workspaces/with_firefox_open/dior all urls.xlsx"
OUTPUT_EXCEL = "/workspaces/with_firefox_open/dior_results.xlsx"
FAILED_URLS_FILE = "/workspaces/with_firefox_open/dior_failed_urls.txt"
CONCURRENT_WORKERS = 1  # request
REQUEST_TIMEOUT = 60
BASE_DELAY = 2.5  
JITTER = 2.0  # rastgele
MAX_RETRIES = 3
BATCH_SIZE = 2  # save
USE_BING_REDIRECT = True  # redirect
BING_CHANCE = 0.6  
SELECTORS = {
    "anchor": "h1.product-detail__name",
    "anchor1": "h1[data-testid=fashion-product-title]",
    "price": ".add-to-cart__price.js-product-price",
    "price1": ".button-price",
    "price2": "#main .mui-latin-obf05p",
    "price3": ".price-line[aria-live=polite]",
    "stock": ".pdp-large-add-to-cart-btn",
    "stock1": ".MuiButton-containedPrimary"
}

class RobustScraper:
    def __init__(self):
        self.processed_urls = set()
        self.failed_urls = set()
        self.retry_counter = {}
        self.proxies = self.load_proxies()
        self.ua = UserAgent()
        self.writer = self.AsyncWriter()
        self.session_counter = 0
        self.last_session_rotation = time.time()
        
    class AsyncWriter:
        def __init__(self):
            self.buffer = []
            self.lock = asyncio.Lock()
            
        async def add(self, data):
            async with self.lock:
                self.buffer.append(data)
                if len(self.buffer) >= BATCH_SIZE:
                    await self.flush()
                    
        async def flush(self):
            if not self.buffer:
                return
            try:
                df = pd.DataFrame(self.buffer)
                if os.path.exists(OUTPUT_EXCEL):
                    try:
                        existing = pd.read_excel(OUTPUT_EXCEL, engine='openpyxl')
                        df = pd.concat([existing, df], ignore_index=True)
                    except Exception as e:
                        logging.warning(f"Var olan dosyayı okuma hatası: {str(e)}. Yeni dosya oluşturuluyor.")
                df.to_excel(OUTPUT_EXCEL, index=False, engine='openpyxl')
                self.buffer.clear()
                logging.info(f"Batch saved: {len(df)} records")
            except Exception as e:
                logging.error(f"Save failed: {str(e)}")
                # yedek dosya
                try:
                    pd.DataFrame(self.buffer).to_excel(f"backup_data_{int(time.time())}.xlsx", index=False)
                    logging.info("Veri yedeklendi")
                except:
                    logging.critical("VERİ KAYBI! Yedekleme başarısız!")
                self.buffer.clear()

    def load_proxies(self):
        if os.path.exists("proxies.txt"):
            with open("proxies.txt") as f:
                return [line.strip() for line in f]
        logging.warning("proxies.txt bulunamadı. Proxy kullanılmayacak.")
        return []

    async def create_session(self):
        # Rastgele User-Agent
        ua = self.ua.random
        
        # HTTP headerlar
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:136.0) Gecko/20100101 Firefox/136.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
        
        self.session_counter += 1
        logging.info(f"Yeni oturum oluşturuldu: #{self.session_counter} - UA: {ua[:30]}...")
        
        return aiohttp.ClientSession(
            headers=headers,
            connector=aiohttp.TCPConnector(ssl=False, limit=CONCURRENT_WORKERS),
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        )
    
    async def get_via_bing(self, session, url):
        """Bing arama sonuçları üzerinden URL'ye erişim sağlar"""
        try:
            # bing arama urli
            search_term = quote_plus(f'site:{url}')
            bing_url = f"https://www.bing.com/search?q={search_term}"
            
            # bingde ara
            async with session.get(bing_url) as response:
                if response.status != 200:
                    logging.warning(f"Bing yanıt vermedi: {response.status}")
                    return None
                
                content = await response.text()
                soup = BeautifulSoup(content, 'lxml')
                
                # ilk url'i bulma ff'i
                search_results = soup.select("li.b_algo h2 a")
                if not search_results:
                    logging.warning(f"Bing sonuç bulamadı: {url}")
                    return None
                
                # ilk sonucu al ve git
                result_url = search_results[0].get('href')
                if not result_url or not result_url.startswith('http'):
                    logging.warning(f"Geçersiz sonuç URL'si: {result_url}")
                    return None
                
                logging.info(f"Bing yönlendirmesi: {url} -> {result_url}")
                
                # aradaki geçiş için bekle
                await asyncio.sleep(random.uniform(1.5, 3.0))
                
                # sonuca git
                async with session.get(result_url) as final_response:
                    if final_response.status == 200:
                        return await final_response.text()
                    else:
                        logging.warning(f"Sonuç URL'si erişim hatası: {final_response.status}")
                        return None
                        
        except Exception as e:
            logging.error(f"Bing yönlendirme hatası: {str(e)}")
            return None

    async def request_with_retry(self, session, url):
        for attempt in range(MAX_RETRIES):
            try:
                # backoff
                delay = BASE_DELAY * (1.5 ** attempt) + random.uniform(0, JITTER)
                await asyncio.sleep(delay)
                
                # random yönlendirme
                use_bing = USE_BING_REDIRECT and random.random() < BING_CHANCE
                
                if use_bing:
                    html = await self.get_via_bing(session, url)
                    if html:
                        return html
                    # Bing başarısız olursa doğrudan iste
                    logging.info(f"Bing başarısız, doğrudan istek deneniyor: {url}")
                
                # varsa proxy seç - yok eklemedim.
                proxy = random.choice(self.proxies) if self.proxies else None
                
                # referrer ekle
                referrers = [
                    "https://www.google.com/",
                    "https://www.bing.com/",
                    "https://duckduckgo.com/"

                ]
                headers = {"Referer": random.choice(referrers)}
                
                async with session.get(url, proxy=proxy, headers=headers) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 403 or response.status == 429:
                        logging.warning(f"Engelleme algılandı (Durum: {response.status}) - {url}")
                        # uzun bekle
                        await asyncio.sleep(random.uniform(5, 10))
                    else:
                        logging.warning(f"Deneme {attempt+1} başarısız: {url} - Durum {response.status}")
            except Exception as e:
                logging.warning(f"Deneme {attempt+1} başarısız: {url}: {type(e).__name__} - {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    return None
        return None

    async def process_single_url(self, session, url, pbar):
        if url in self.processed_urls:
            pbar.update(1)
            return
            
        try:
            html = await self.request_with_retry(session, url)
            if not html:
                self.failed_urls.add(url)
                pbar.update(1)
                return
                
            soup = BeautifulSoup(html, 'lxml')
            data = {"url": url}
            
            for key, selector in SELECTORS.items():
                try:
                    element = soup.select_one(selector)
                    data[key] = element.get_text(strip=True) if element else "N/A"
                except Exception as e:
                    data[key] = f"ERROR: {str(e)}"
                    logging.error(f"Selector error {key} @ {url}")
                
            await self.writer.add(data)
            self.processed_urls.add(url)
            
        except Exception as e:
            logging.error(f"Processing failed for {url}: {str(e)}")
            self.failed_urls.add(url)
        finally:
            pbar.update(1)

    async def worker(self, queue, pbar):
        # her istek için ayrı oturum
        session = await self.create_session()
        async with session:
            while True:
                try:
                    url = await queue.get()
                    
                    # random oturum yenile
                    if random.randint(1, 50) == 1 or (time.time() - self.last_session_rotation > 600):
                        self.last_session_rotation = time.time()
                        await session.close()
                        session = await self.create_session()
                        logging.info("Oturum yenilendi")
                    
                    try:
                        await self.process_single_url(session, url, pbar)
                    finally:
                        queue.task_done()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logging.error(f"Worker hatası: {str(e)}")
                    queue.task_done()

    async def run(self):
        df = pd.read_excel(INPUT_EXCEL)
        urls = df.iloc[:, 0].dropna().astype(str).unique().tolist()
        
        # urlleri random karıştır  
        random.shuffle(urls)
        
        total_urls = len(urls)
        print(f"Toplam URL: {total_urls}")
        
        queue = asyncio.Queue(maxsize=CONCURRENT_WORKERS*2)
        pbar = tqdm_asyncio(total=total_urls, desc="Scraping Progress", unit="url")
        
        # devam eden varsa, yüklemeye devam et.
        if os.path.exists(OUTPUT_EXCEL):
            try:
                existing_df = pd.read_excel(OUTPUT_EXCEL)
                self.processed_urls = set(existing_df['url'].unique())
                print(f"Devam ediliyor: {len(self.processed_urls)} URL zaten işlenmiş")
            except Exception as e:
                logging.error(f"Progress yüklenirken hata: {str(e)}")
        
        # success olmayanları da yükle varsa
        if os.path.exists(FAILED_URLS_FILE):
            try:
                with open(FAILED_URLS_FILE, 'r') as f:
                    self.failed_urls = set(line.strip() for line in f)
                print(f"{len(self.failed_urls)} başarısız URL yüklendi")
            except Exception as e:
                logging.error(f"Başarısız URL'ler yüklenirken hata: {str(e)}")
        
        workers = []
        try:
            # istekleri başlat
            workers = [asyncio.create_task(self.worker(queue, pbar)) 
                      for _ in range(CONCURRENT_WORKERS)]
            
            # kuyruğa al
            for url in urls:
                if url not in self.processed_urls and url not in self.failed_urls:
                    await queue.put(url)
            
            # hepsi işlenene kadar bekle
            await queue.join()
            
        except KeyboardInterrupt:
            print("\nKullanıcı tarafından durduruldu. Mevcut veriler kaydediliyor...")
        finally:
            # cache temizle
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            
            #  verileri kaydet
            await self.writer.flush()
            
            # success olmayanları da kaydet
            if self.failed_urls:
                with open(FAILED_URLS_FILE, 'w') as f:
                    f.write("\n".join(self.failed_urls))
                print(f"\n{len(self.failed_urls)} URL başarısız oldu. Detaylar: {FAILED_URLS_FILE}")
            
            print(f"İşlem tamamlandı: {len(self.processed_urls)}/{total_urls} URL işlendi.")

if __name__ == "__main__":
    logging.basicConfig(
        filename='scraper.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='w'
    )
    
    scraper = RobustScraper()
    try:
        asyncio.run(scraper.run())
    except KeyboardInterrupt:
        print("\nProgram kapatıldı.")
