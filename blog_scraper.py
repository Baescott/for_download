import os
import requests

import re
import pytz
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from datetime import timedelta
from bs4 import BeautifulSoup


class BlogScraper(object):

    def __init__(self):
        if not os.path.exists("results_excel"): os.makedirs("results_excel")
        if not os.path.exists("results_csv"): os.makedirs("results_csv")
        
        self.PROXIES = {'http': 'http://84.1.0.151:3128',
                        'https': 'http://84.1.0.151:3128'}
        self.TIMEZONE = pytz.timezone('Asia/Seoul')
        self.COOKIES = {
            'NNB': 'XSJGWOG5YCRGE',
            'NID_AUT': 'NpjYjyYBU43Z4KwAxYQuhY2QEILySaTmIrkKn4D/Q6d7UtTfOj11tovW97k1kqoi',
            'NID_JKL': 'ncQXUd+Vz2T7VBpz6iGejMJanCHm5SbwBFcFKVp0tTc=',
            '_ga': 'GA1.2.740119997.1659410660',
            'ASID': 'd3b2aeb300000182c7f14a9e00000055',
            'nx_ssl': '2',
            'NID_SES': 'AAABxyI/SlgT4janJNmk7o7MB8fCz1A9ZECe4SQESHxziHLeIH9T7VXmLeuo1K+WP3lO+'
                       'wcsu0zGnlIN56r23KsTDVJ7rQ6MQKiTWEKz7n1vyEoaW196skpF6c+/1wrwtQYowHCI0f'
                       '11RK8NplOu/hELKpx9aSxbLIzyibm9dVrHEhd4VXnR/ryZ8Sov6zjfq37poBd1XO8I4Zd'
                       'B4WLnS+Yt2PpmPlZWHoakcR9glEnxSveLNhoJNJ5oAMIMKqXfgHOIVWoqjwkYr+eP+00Y'
                       'OeqPxUWst8HDnQPRwi+bTd2uzjwMJGnyP4uMHk9dsUXQW71DH2HovUUfFvAhJNkdjZRNZ'
                       'DoMGwG0suc7SxQcI7jgQQsuiFoIHPq4yuMXjt7zv4TwCiIzJBQu0sLEkY+OhSFM1Zlv/v'
                       'KX+glSNnh8tBOGj4o4xh4nNdtWbZCPjrykyeCmWbPeSDsVMsGsKOStFrPYweqtOUmQWOc'
                       'gR/RwHdTR1GvAXy45Ze5qLaOviIDLozAjkbuJS2EKQ+WE9IWpji8BXNVuHb/xZPQoU1QQ'
                       'EXWegZF5tlXvUPDVeMTPzXch6JbJ367dnrlLaQeWi/9YLC1nKYOJ92IWI/VSwfd77TZvCSDR',
            'page_uid': 'iRbqHsqo15VsstdNcO8ssssstYC-295983',
        }

        self.HEADERS = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,'
                      'image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
            'sec-ch-ua-arch': '"arm"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-full-version-list': '"Chromium";v="118.0.5993.117", '
                                           '"Google Chrome";v="118.0.5993.117", "Not=A?Brand";v="99.0.0.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"macOS"',
            'sec-ch-ua-platform-version': '"13.0.1"',
            'sec-ch-ua-wow64': '?0',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        }

    def get_search_list(self,
                        keyword: str,
                        from_date: int = 0,
                        to_date: int = 0,
                        max_page: int = 10):

        cookies = self.COOKIES
        headers = self.HEADERS
        headers['authority'] = 'search.naver.com'
        params = {'query': keyword,
                  'where': 'blog',
                  'sm': 'tab_opt',
                  'start': 1}
        if from_date != 0 and to_date != 0:
            params.update({'nso': f'so:dd,p:from{from_date}to{to_date}'})

        search_list = []

        for i in tqdm(range(max_page)):
            blog_search_response = requests.get('https://search.naver.com/search.naver',
                                                params=params,
                                                cookies=cookies,
                                                headers=headers,
                                                proxies=self.PROXIES)
            blog_search_response_html = BeautifulSoup(blog_search_response.text, 'html.parser')
            blog_search_results = blog_search_response_html.find_all('li', id=re.compile("blog"))

            if i == 0:
                search_chunk = len(blog_search_results)
            else:
                params['start'] = 1 + (search_chunk * i)

            for j, blog_search_result in enumerate(blog_search_results):
                blog_writer = blog_search_result.find('a', class_='name').text
                blog_title = blog_search_result.find('a', class_='title_link').text
                blog_url = blog_search_result.find('a', class_='title_link')['href']
                blog_id = blog_url.split('/')[3]
                blog_num = blog_url.split('/')[-1]

                if 'blog.naver.com' in blog_url:
                    search_list.append({'blog_writer': blog_writer,
                                        'blog_title': blog_title,
                                        'blog_url': blog_url,
                                        'blog_id': blog_id,
                                        'blog_num': blog_num})

        return search_list

    def get_blog_dataframe(self,
                           keyword: str,
                           from_date: int = 0,
                           to_date: int = 0,
                           max_page: int = 10,
                           is_save: bool = True):
        search_list = self.get_search_list(keyword=keyword,
                                           from_date=from_date,
                                           to_date=to_date,
                                           max_page=max_page)

        cookies = self.COOKIES
        headers = self.HEADERS
        
        for i, content in tqdm(enumerate(search_list)):
            try:
                if 'blog.naver.com' not in content['blog_url']:
                    content.update({'year': '',
                                    'month': '',
                                    'day': '',
                                    'hour': '',
                                    'minute': '',
                                    'text': ''})
                else:
                    headers['referer'] = content['blog_url']
                    params = {'blogId': content['blog_id'], 'logNo': content['blog_num']}

                    blog_response = requests.get('https://blog.naver.com/PostView.naver',
                                                 params=params,
                                                 cookies=cookies,
                                                 headers=headers,
                                                 proxies=self.PROXIES)
                    blog_response_html = BeautifulSoup(blog_response.text, 'html.parser')

                    blog_text = self.get_blog_text(blog_response_html)
                    pub_year, pub_month, pub_day, pub_hour, pub_min = self.get_published_datetime(blog_response_html)

                    content.update({'year': pub_year,
                                    'month': pub_month,
                                    'day': pub_day,
                                    'hour': pub_hour,
                                    'minute': pub_min,
                                    'text': blog_text})
            except Exception as e:
                content.update({'year': '',
                                    'month': '',
                                    'day': '',
                                    'hour': '',
                                    'minute': '',
                                    'text': ''})
                
                # print(f"{e} from {content['blog_url']}")

        search_result = pd.DataFrame(search_list)

        if is_save:
            if from_date != 0 and to_date != 0:
                search_result.to_excel(f"results_excel/blog_{keyword}_from_{from_date}_to_{to_date}.xlsx", index=False)
                search_result.to_csv(f"results_csv/blog_{keyword}_from_{from_date}_to_{to_date}.csv", encoding='utf-8', index=False)
            else:
                today = f"{datetime.now(self.TIMEZONE).year}" \
                        f"{datetime.now(self.TIMEZONE).month}" \
                        f"{datetime.now(self.TIMEZONE).day}"
                search_result.to_excel(f"results_excel/blog_{keyword}_til_{today}.xlsx", index=False)
                search_result.to_csv(f"results_csv/blog_{keyword}_til_{today}.csv", encoding='utf-8', index=False)
        
        return search_result

    def get_published_datetime(self, blog_response_html):
        try:
            pub_datetime = blog_response_html.find('span', class_=re.compile('publishDate')).text
        except Exception as e:
            pub_datetime = blog_response_html.find('p', class_=re.compile('_postAddDate')).text

        if '.' in pub_datetime:
            pub_date = [x.strip() for x in pub_datetime.split('.')[:-1]]
            pub_year, pub_month, pub_day = pub_date[0], pub_date[1], pub_date[2]
            pub_time = pub_datetime.split('.')[-1].strip()
            pub_hour, pub_min = pub_time.split(':')[0], pub_time.split(':')[-1]
        else:
            gap = int(re.sub('[^0-9]', '', pub_datetime))
            real_datetime = datetime.now(self.TIMEZONE)
            pub_dt_renewed = real_datetime - timedelta(hours=gap) if '시간' in pub_datetime \
                else real_datetime - timedelta(minutes=gap) if '분' in pub_datetime \
                else real_datetime - timedelta(seconds=gap)
            pub_year, pub_month, pub_day = pub_dt_renewed.year, pub_dt_renewed.month, pub_dt_renewed.day
            pub_hour, pub_min = pub_dt_renewed.hour, pub_dt_renewed.minute
        
        return pub_year, pub_month, pub_day, pub_hour, pub_min

    def get_blog_text(self, blog_response_html):
        text_1 = blog_response_html.find_all('div', class_=re.compile("se-module-text"))
        
        if len(text_1) != 0:
            result = ' '.join([self.cleanse_text(x.text) for x in text_1][1:]).strip()
        else:
            text_2 = blog_response_html.find('div', id=re.compile('postViewArea'))
            if text_2 is None:
                result = self.cleanse_text(blog_response_html.find('p', class_=re.compile('se_textarea')).text)
            else:
                result = self.cleanse_text(text_2.text)
        return result

    def cleanse_text(self, text_chunk: str):
        return text_chunk.replace('\n', '').replace('\u200b', ' ').replace('\xa0', ' ').strip()
