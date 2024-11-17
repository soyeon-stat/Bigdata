# id
# community string
# link href
# category string
# title string
# vote none or integer
# view integer
# comment integer
# date yyyy-mm-dd hh:mm
# content string
# comments list or array

import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import time
import json

GASENGI_BASE_URL = 'http://www.gasengi.com/main/board.php?bo_table='
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Accept-Encoding": "gzip, deflate, br"
}
CATEGORY_INFO = [
    {'raw_table_code': 'commu08', 'table_code': 'chat', 'kor_name': '잡담'},
    {'raw_table_code': 'commu_etn', 'table_code': 'entertainment', 'kor_name': '방송'},
    {'raw_table_code': 'military', 'table_code': 'military', 'kor_name': '군사'},
    {'raw_table_code': 'politics_bbs03', 'table_code': 'politics', 'kor_name': '정치'},
    {'raw_table_code': 'football04', 'table_code': 'football', 'kor_name': '축구'},
    ]

def get_past_dates(n: int = 2) -> list:
    today = datetime.now().date()
    date_list = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n)]
    return date_list

def date_converter(date: str) -> str:
    if isinstance(date, str):
        input_date = datetime.strptime(date, "%Y-%m-%d").date()
    elif isinstance(date, dt_date):
        input_date = date
    elif isinstance(date, pd.Timestamp):
        input_date = date.date()
    else:
        raise ValueError('no instance match')
    
    return input_date.strftime("%m-%d")

def get_table_code(raw_code):
    for category in CATEGORY_INFO:
        if category['raw_table_code'] == raw_code:
            return category['table_code']
        
def process_subject(subject_td) -> dict:
    a_list = subject_td.find_all('a')
    new_a_tag = []
    for a_tag in a_list:
        href = a_tag.get('href')
        if 'wr_id' in href:
            new_a_tag.append(a_tag)
    if new_a_tag[0].find('span', class_='notice'):
        return None
    else:
        return process_a(new_a_tag)
    
def process_a(new_a_tag) -> dict:
    dict_temp = initial_dict()
    dict_temp['id'] = re.search(r'wr_id=(\d+)', new_a_tag[0]['href']).group(1)
    dict_temp['title'] = new_a_tag[0].text.strip()
    dict_temp['comment'] = 0
    if len(new_a_tag) > 1:
        dict_temp['comment'] = int(re.search(r'\((\d+)\)', new_a_tag[1].text).group(1))
    return dict_temp

def get_articles(date_list: list, category_info_dict: dict) -> dict:
    check_date_list = [date_converter(date) for date in date_list]
    dict_canditates = []
    continue_to_next = True
    page_num = 1
    raw_table_code = category_info_dict.get('raw_table_code')
    table_code = category_info_dict.get('table_code')
    print(category_info_dict.get('kor_name'))

    while continue_to_next:
        url = f"{GASENGI_BASE_URL}{raw_table_code}&page={page_num}"
        resp = requests.get(url, headers=headers)
        soup = BeautifulSoup(resp.text, 'html.parser')
        bg_rows = soup.find_all('tr', {'class': ['bg0', 'bg1']})

        for bg in bg_rows:
            dict_temp = process_subject(bg.find('td', class_='subject'))
            if dict_temp:
                dict_temp['community'] = 'gasengi'
                dict_temp['category'] = table_code
                dict_temp['view'] = bg.find('td', class_='hit').text
                dict_temp['link'] = url + f"&wr_id={dict_temp.get('id')}"
                dict_temp['vote'] = None
                
                datetext = bg.find('td', class_='datetime').text
                if ':' in datetext:
                    datetext = datetime.now().date().strftime("%m-%d")
                if datetext in check_date_list:
                    
                    dict_result = get_contents(dict_temp)
                    time.sleep(0.2)
                    if dict_result is not None:
                        dict_canditates.append(dict_result)
                else:
                    continue_to_next = False
        page_num += 1
        time.sleep(1)
    print()
    return dict_canditates

def get_contents(dict_temp):
    resp = requests.get(dict_temp.get('link'), headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')

    if dict_temp.get('title')[-1] == '…':
        title_temp = soup.find('div', style="color:#505050; font-size:13px; font-weight:bold; word-break:break-all;").get_text(strip=True)
        start_index = title_temp.find(dict_temp.get('title')[:-1])
        dict_temp['title'] = title_temp[start_index:]

    dict_temp['date'] = datetime.strptime(soup.find('span', style="color:#888888;").get_text(strip=True).replace('작성일 : ', ''), '%y-%m-%d %H:%M').strftime('%Y-%m-%d %H:%M')

    content_temp = soup.find(id="writeContents_sier").find_all('div')
    content_text = ''
    for line in content_temp:
        if line.text.strip() and not line.text.strip().startswith('출처 :'):
            text_temp = line.text.strip().replace('\n\n', ' ').replace('\n', ' ').replace('\xa0', ' ').replace('\r', ' ')
            content_text += text_temp
            content_text += ' '
        
    dict_temp['content'] = content_text.strip()

    comments = soup.find_all('div', style="line-height:20px; padding:7px; word-break:break-all; overflow:hidden; clear:both; ")
    comments_list = []
    for comment in comments:
        if comment.text.strip():
            text_temp = comment.text.strip().replace('\n\n', ' ').replace('\n', ' ').replace('\xa0', ' ').replace('\r', ' ')
            comments_list.append(text_temp.strip())
    dict_temp['comments'] = comments_list
    dict_temp['comment'] = len(comments_list)

    if dict_temp['content']:
        print(f"{dict_temp['title']}")
        return dict_temp
    else:
        return None
    
def initial_dict():
    return {
        'id': None,
        'community': None,
        'link': None,
        'category': None,
        'title': None,
        'vote': None,
        'view': None,
        'comment': None,
        'date': None,
        'content': None,
        'comments': None
    }
    
if __name__ == "__main__":
    list_result = []
    for dict_category in CATEGORY_INFO:
        result_temp = get_articles(get_past_dates(7), dict_category)
        list_result.extend(result_temp)
    with open("241113_gasengi_crawling_result.json", "w", encoding="utf-8") as file:
        json.dump(list_result, file, ensure_ascii=False, indent=4)