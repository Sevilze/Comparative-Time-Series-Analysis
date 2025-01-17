from playwright.sync_api import sync_playwright
from datetime import datetime, timedelta
from dateutil import parser
import os
import csv
import sys
import time
from pathlib import Path
from PIL import Image
import matplotlib.pyplot as plt

sys.stdout.reconfigure(encoding='utf-8')

class BMKGScraper:
    def __init__(self, email, institution):
        self.email = email
        self.institution = institution
        self.output_file = 'final_earthquake_data.csv'
        self.headers = [
            "EventID","DateTime", "Latitude", "Longitude", "Magnitude", 
            "MagType", "Depth", "PhaseCount", "AzimuthGap", "Location", "Agency"
        ]
        
    def solve_captcha_manually(self, page):
        try:
            captcha_dir = Path('captcha')
            if captcha_dir.exists():
                for file in captcha_dir.glob("*.png"):
                    file.unlink()

            captcha_img = page.locator('.captcha img')
            if not Path('captcha').exists():
                Path('captcha').mkdir()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            img_path = f'captcha/captcha_{timestamp}.png'
            captcha_img.screenshot(path=img_path)
            solution = input("CAPTCHA Solution: ")
            time.sleep(0.5)
            return solution
        except Exception as e:
            print(f"Error handling CAPTCHA: {str(e)}")
            return None
        
    def get_last_datetime_from_csv(self):
        if not os.path.exists(self.output_file):
            return None
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
                if len(rows) > 1:
                    last_row = rows[-1]
                    return datetime.strptime(last_row[1], '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(f"Error reading the last datetime from CSV: {str(e)}")
        return None

    def fill_search_form(self, page, start_date, next_date):
        try:
            page.click('input[id="custom"]')
            page.fill('input[name="min_date"]', start_date.strftime('%Y-%m-%dT%H:%M:%S'))
            
            page.fill('input[name="minmag"]', "0.0")
            page.fill('input[name="maxmag"]', "10.0")

            page.fill('input[name="mindepth"]', "0")
            page.fill('input[name="maxdepth"]', "1000")

            page.fill('input[name="north"]', "6")
            page.fill('input[name="south"]', "-11")
            page.fill('input[name="west"]', "95")
            page.fill('input[name="east"]', "141")
            
            page.check('input[value="preliminaryeq"]')  
            page.fill('input[name="max_date"]', next_date.strftime('%Y-%m-%dT%H:%M:%S'))

            page.fill('input[name="email"]', self.email)
            page.fill('input[name="institution"]', self.institution)
        
            return True
        except Exception as e:
            print(f"Error filling search form: {str(e)}")
            return False

    def extract_table_data(self, page):
        data = []
        try:
            page.wait_for_selector('table', timeout=60000)
            page.select_option('select[name="example2_length"]', '100')

            while True:
                page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                rows = page.query_selector_all('table tr')
                for row in rows:
                    cells = row.query_selector_all('td')
                    if cells:
                        row_data = [cell.inner_text().strip() for cell in cells]
                        row_data[2] = datetime.fromisoformat(row_data[2].replace("Z", "+00:00")).strftime('%Y-%m-%d %H:%M:%S')

                        print("Extracted Row:", row_data)
                        data.append(row_data)
            
                next_button = page.query_selector('a.page-link[aria-controls="example2"]:has-text("Next")')
                if next_button and 'disabled' not in page.query_selector('li.paginate_button.next').get_attribute('class'):
                    next_button.click()
                    time.sleep(1)
                else:
                    break
        
            return data
        except Exception as e:
            print(f"Error extracting table data: {str(e)}")
            return []

    def save_to_csv(self, data, mode='a'):
        try:
            if data:
                data.sort(key=lambda row: datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S'))
            
            with open(self.output_file, mode, newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if mode == 'w':
                    writer.writerow(self.headers)
                for row in data:
                    writer.writerow(row[1:])
        except Exception as e:
            print(f"Error saving to CSV: {str(e)}")

    def scrape(self, start_date=None, end_date=None):
        last_datetime = self.get_last_datetime_from_csv()
        if last_datetime:
            print(f"Resuming from last datetime in CSV: {last_datetime}")
            start_date = last_datetime + timedelta(seconds=1)

        if not start_date:
            start_date = datetime(2008,11,1)
        if not end_date:
            end_date = datetime.now()

        current_date = start_date
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()
            
            # if not last_datetime:
            #     self.save_to_csv([], mode='w')
            
            while current_date < end_date:
                next_date = min(current_date + timedelta(days=30), end_date)
                print(f"\nProcessing period: {current_date.date()} to {next_date.date()}")
                
                retry_count = 0
                max_retries = 3
                
                while retry_count < max_retries:
                    try:
                        page.goto('https://repogempa.bmkg.go.id/eventcatalog')
                        time.sleep(1)
                        
                        if not self.fill_search_form(page, current_date, next_date):
                            raise Exception("Failed to fill search form")
                        
                        captcha_solution = self.solve_captcha_manually(page)
                        if not captcha_solution:
                            break
                        page.fill('input[name="captcha"]', captcha_solution)
                        page.click('button[type="submit"]')

                        data = self.extract_table_data(page)
                        if data:
                            self.save_to_csv(data, mode='a')
                            break
                        else:
                            raise Exception("No data found in table")
                            
                    except Exception as e:
                        retry_count += 1
                        print(f"Attempt {retry_count} failed: {str(e)}")
                        if retry_count == max_retries:
                            print(f"Failed to process period after {max_retries} attempts")
                
                current_date = next_date
            browser.close()

input_csv = 'earthquake_data.csv'
output_csv = 'final_earthquake_data.csv'

if __name__ == "__main__":
    scraper = BMKGScraper(
        email="Abduli@gmail.com",
        institution="UI"
    )
    scraper.scrape()

        