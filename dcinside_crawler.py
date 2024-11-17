import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import time
import json

DCINSIDE_BASE_URL = 'https://gall.dcinside.com/board/lists/?id=dcbest&page='
DCINSIDE_ARTICLE_URL = 'https://gall.dcinside.com/board/view/?id=dcbest&no='
DCINSIDE_COMMENT_URL = 'https://gall.dcinside.com/board/comment/'
MAX_RETRIES = 5
RETRY_DELAY = 1

HEADERS_FOR_LIST = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'ko-KR,ko;q=0.9',
    'Referer': 'https://gall.dcinside.com/board/lists/?id=dcbest&page=1&_dcbest=1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
    }

HEADERS_FOR_COMMENT = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'ko-KR,ko;q=0.9',
    'Referer': 'https://gall.dcinside.com/board/lists/?id=dcbest&page=1&_dcbest=1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
    }

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
    
    return input_date.strftime("%m.%d")

def initial_dict():
    return {
        'id': None,
        'community': 'dcinside',
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

def get_contents(dict_temp):
    resp = requests.get(dict_temp.get('link'), headers=HEADERS_FOR_LIST)
    soup = BeautifulSoup(resp.text, 'html.parser')
    dict_temp['title'] = soup.find('span', class_='title_subject').text.strip()
    dict_temp['view'] = int(soup.find('span', class_='gall_count').text[3:].replace(',', '').strip())
    dict_temp['vote'] = int(soup.find('span', class_='gall_reply_num').text[3:].replace(',', '').strip())
    dict_temp['comment'] = int(soup.find('span', class_='gall_comment').text[3:].replace(',', '').strip())
    dict_temp['date'] = datetime.strptime(soup.find('span', class_='gall_date').text.strip(), '%Y.%m.%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
    print(f"{dict_temp['title']}")

    # content_temp = soup.find('div', class_='write_div').find_all(lambda tag: tag.name in ['div', 'p'])
    content_temp = [
        element for element in soup.find('div', class_='write_div').contents 
        if element.name in ['div', 'p']
    ]
    content_text = ''
    for content in content_temp[:-1]:
        if content.text:
            content_text += content.text.replace('\n\n', ' ').replace('\n', ' ').replace('\xa0', ' ').replace('\r', ' ')
            content_text += ' '
    
    dict_temp['content'] = content_text.strip()

    end_index = content_temp[-1].text.find(' 갤러리 [원본 보기]')
    dict_temp['category'] = content_temp[-1].text[4:end_index].strip()
    dict_temp = get_comments(dict_temp)
    dict_temp['comment'] = len(dict_temp['comments'])

    if dict_temp['content']:
        return dict_temp
    else:
        return None
    
def fetch_comments_with_retry(payload):
    attempt = 0
    while attempt < MAX_RETRIES:
        response = requests.post(DCINSIDE_COMMENT_URL, data=payload, headers=HEADERS_FOR_COMMENT)
        
        # 상태 코드와 JSON 형식 여부 확인
        if response.status_code == 200 and response.headers.get('Content-Type') == 'text/html; charset=UTF-8':
            try:
                comments_temp = response.json().get('comments')
                return comments_temp  # 성공적으로 JSON 파싱 완료
            except requests.JSONDecodeError:
                print(f"JSONDecodeError 발생 (시도 {attempt + 1}/{MAX_RETRIES})")
        else:
            print(f"Error: 상태 코드 {response.status_code} 또는 JSON 형식이 아님 (시도 {attempt + 1}/{MAX_RETRIES})")

        # 재시도 대기 후 다시 호출
        attempt += 1
        time.sleep(RETRY_DELAY)

    # 최대 재시도 횟수 초과 시 오류 반환
    print("Error: 최대 재시도 횟수를 초과했습니다.")
    return None
    
def get_comments(dict_temp):
    page_num = 1
    continue_to_next_page = True
    comments_list = []
    html_tag_pattern = re.compile(r'<(div|img|video|a)[^>]*>')

    while continue_to_next_page:
        payload = {
            "id": "dcbest",
            "no": dict_temp['id'],
            "cmt_id": "dcbest",
            "cmt_no": dict_temp['id'],
            "focus_cno": "",
            "focus_pno": "",
            "e_s_n_o": "3eabc219ebdd65f539",
            "comment_page": page_num,
            "sort": "D",
            "prevCnt": 0,
            "board_type": "",
            "_GALLTYPE_": "G"
        }
        # response = requests.post(DCINSIDE_COMMENT_URL, data=payload, headers=HEADERS_FOR_COMMENT)
        # comments_temp = response.json().get('comments')
        comments_temp = fetch_comments_with_retry(payload)

        
        if comments_temp:
            filtered_comments = [x for x in comments_temp if not html_tag_pattern.search(x['memo'])]
            for x in filtered_comments:
                if x['nicktype'] != 'COMMENT_BOY':
                    comment_temp = x['memo'].replace('\n\n', ' ').replace('\n', ' ').replace('\xa0', ' ').replace('\r', ' ').replace('- dc App', '').strip()
                    if comment_temp != '해당 댓글은 삭제되었습니다.':
                        comments_list.append(comment_temp)
            page_num += 1
            time.sleep(0.1)
        else:
            continue_to_next_page = False
    comments_list = list(dict.fromkeys(comments_list))
    dict_temp['comments'] = comments_list
    return dict_temp

def get_articles(date_list: list) -> dict:
    check_date_list = [date_converter(date) for date in date_list]
    page_num = 1
    continue_to_next = True
    dict_canditates = []

    while continue_to_next:
        url = f"{DCINSIDE_BASE_URL}{page_num}&_dcbest=1"
        resp = requests.get(url, headers=HEADERS_FOR_LIST)
        soup = BeautifulSoup(resp.text, 'html.parser')
        tr_tags = soup.find_all('tr', class_='ub-content us-post thum')
        # print(tr_tags)
        for tr in tr_tags:
            dict_temp = initial_dict()
            dict_temp['id'] = tr.find('td', class_='gall_num').text.strip()
            dict_temp['link'] = f"{DCINSIDE_ARTICLE_URL}{dict_temp['id']}&_dcbest=1"

            datetext = tr.find('td', class_='gall_date').text.strip()
            if ':' in datetext:
                datetext = datetime.now().date().strftime("%m.%d")
            if datetext in check_date_list:
                dict_result = get_contents(dict_temp)
                time.sleep(0.2)
                if dict_result is not None:
                    dict_canditates.append(dict_result)
            else:
                continue_to_next = False
        page_num += 1
        time.sleep(1)
    return dict_canditates

if __name__ == "__main__":
    list_result = get_articles(get_past_dates(7))
    with open("241113_dcinside_crawling_result.json", "w", encoding="utf-8") as file:
        json.dump(list_result, file, ensure_ascii=False, indent=4)