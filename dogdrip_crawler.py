import pandas as pd
import requests
import time
import json
import pytz
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# timezone 고정
tz = pytz.timezone('Asia/Seoul')


## Default Setting
def fetch_find_post_date(row, now):
    """
    게시판의 행을 읽어서 날짜를 추출하는 함수
    X시간 전으로 표기된 경우들을 처리함
    :param row: 날짜를 추출할 행 (bs4.Beasutifulsoup)
    :param now: 크롤링을 한 날짜 (datetime.datetime)
    :return: 추출한 날짜 (datetime.datetime)
    """

    # 글머리에 해당하는 경우
    if len(row.find_all('th')) > 0:
        return None

    # 글 등록시간
    dates = row.find('td', {'class': 'time'}).text
    if "분" in dates:
        minutes = int(re.sub("\D+", "", dates))
        dates = now - timedelta(minutes=minutes)

    elif "시간" in dates:
        hours = int(re.sub("\D+", "", dates))
        dates = now - timedelta(hours=hours)

    elif "일" in dates :
        days = int(re.sub("\D+","", dates))
        dates = now - timedelta(days = days)
        dates = pd.to_datetime(dates.strftime('%Y-%m-%d'))
    else:
        dates = pd.to_datetime(dates)

    return dates


def fetch_post_information(row, now, end_date) :
    """
    게시판 페이지에 등재되어 있는 글들을 읽어오는 함수
    :param row: 게시판 행(bs4.Beasutifulsoup)
    :param now: 현재 날짜(datetime.datetime)
    :param end_date: 크롤링 마지막 시간(string, yyyy-mm-dd 형식)
    :return: {'post_no': 게시글, 'title': 글 제목, 'author': 게시자,
             'vote': 추천수, 'dates': 게시날짜}
    """

    dates = fetch_find_post_date(row, now)

    if dates :

        # 사전에 정한 기간 (크롤링 시간으로부터 일주일)을 넘어서면 for문 중단
        if dates.strftime('%Y-%m-%d') < end_date :
            return None

        # 글제목, 글번호
        title_row = row.find('td', {'class': 'title'})
        title = title_row.find('span', {'class': 'ed title-link'}).text

        href = title_row.find('a')['href']
        post_no = href.split("?")[0].split("/")[-1]

        # 글쓴이
        author = row.find('td', {'class': 'author'}).text.strip()

        # 추천수
        vote = row.find('td', {'class': 'ed voteNum text-primary'}).text

        return {'post_no': post_no,
                 'title': title,
                 'author': author,
                 'vote': vote,
                 'dates': dates
                 }

def fetch_board_list(page, days = 7) :
    """
    게시판의 게시글 정보를 모으는 함수
    :param page: 게시글을 불러올 페이지 (integer)
    :param days: 크롤링할 기간. 기본값은 7일. (integer)
    :return: post_data (pandas.dataframe)
    """

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
    }
    url = f'https://www.dogdrip.net/dogdrip?sort_index=popular&page={page}'
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    board_list = soup.find('div', {'class' : 'ed board-list'})

    now = datetime.now(tz)
    end_date = pd.to_datetime(now - timedelta(days = days))
    end_date = end_date.strftime('%Y-%m-%d')

    post_data = pd.DataFrame({})
    rows = board_list.find_all('tr')

    try :
        for row in rows :
            dates = fetch_find_post_date(row, now)
            if dates :
                if dates.strftime('%Y-%m-%d') < end_date :
                    continue

                post_info = fetch_post_information(row, now, end_date)
                post_info = pd.DataFrame(post_info, index = [0])
                post_data = pd.concat([post_data, post_info])
    finally :
        return post_data

def fetch_comment_contents(post_no, comment_page) :
    """
    게시글의 댓글을 불러오는 함수
    :param post_no: 게시글 번호 (integer)
    :param comment_page: 게시글이 페이징 되어 있을 때 불러올 댓글의 페이지 (integer)
    :return: comments (list)
    """

    comments = []

    for c_page in range(comment_page) :

        c_params = {'document_srl': post_no,
                   'cpage' : str(c_page+1),
                   'module': 'board',
                   'act': 'getBoardCommentsData'}

        c_header = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                    'Referer' : f'https://www.dogdrip.net/dogdrip/{post_no}',
                    'X-Ajax-Compat' : 'JSON',
                    'X-Requested-With' : 'XMLHttpRequest'
        }

        c_response = requests.post(url = 'https://www.dogdrip.net',
                      params= c_params,
                      headers=c_header
                      )
        c_html = BeautifulSoup(c_response.json()['html'], 'html.parser')

        comment_tags = c_html.find_all('div', {'class' : 'ed comment-content'})

        for c in comment_tags :
            comment = c.select("[class*='comment']")[-1].text
            comments.append(comment)

    return comments


def find_last_comment_page(soup) :
    """
    게시글의 댓글이 페이징 되어 있는 경우, 마지막 페이지를 추출하는 함수
    :param soup: 게시글의 soup (bs4.Beautifulsoup)
    :return: 게시글 내 댓글의 마지막 페이지(integer)
    """
    ul = soup.find('div', {'id' : 'commentbox'}).find('ul', {'class' : 'ed pagination pagewide'})
    if ul :
        return len(ul.find_all('li'))
    else :
        return 1

def fetch_post_content(post_no):
    """
    게시글의 본문과 댓글을 불러오는 함수
    :param post_no: 불러오고자 하는 댓글 번호(string)
    :return: {'post_no': 게시글 번호(string)
            'comment': 댓글 개수(integer),
            'content': 본문(string),
            'comments': 댓글내용(list)}
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
    }
    url = f'https://www.dogdrip.net/dogdrip/{post_no}'
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    container = soup.find('div', {'class': 'ed'})

    # 본문
    document = soup.select("[class*='document']")
    body = ""
    for line in document[1]:
        try:
            if len(line.text.strip()) > 1:
                body += line.text.strip() + "\n"
        except:
            continue

    # 댓글 수
    n_comments = container.find('h4', {'class': 'ed comment-header'}).text.strip()
    n_comments = int(re.sub("\D", "", n_comments))

    # 댓글 내용
    last_c_page = find_last_comment_page(soup)
    comments = fetch_comment_contents(post_no, last_c_page)

    return {'post_no':  post_no,
            'comment':  n_comments,
            'content':  body,
            'comments': comments,
            'updated' : datetime.now().strftime('%Y-%m-%d %H:%M'),
            }

if __name__ == "__main__" :

    # 게시글 목록 리스팅
    page = 1
    days = 7        # 크롤링할 기간(N일 전부터 오늘까지)
    contents = []

    while True :

        # 게시판 페이지 글 리스트업
        post = fetch_board_list(page, days)
        if post.shape[0] > 0 :

            for idx, v in post.iterrows() :
                post_no, title, author, vote, dates = v
                # 본문 & 댓글 크롤링
                article_comments = fetch_post_content(post_no)

                result = {
                    'ID': f'dogdrip-{post_no}',
                    'community': '개드립넷',
                    'link': f'https://www.dogdrip.net/dogdrip/{post_no}',
                    'category': '개드립 인기글',
                    'title': title,
                    'vote': vote,
                    'view': None,
                    'comment': article_comments['comment'],
                    'date': dates.strftime('%Y-%m-%d %H:%M'),
                    'content': article_comments['content'],
                    'comments': article_comments['comments'],
                    'updated' : article_comments['updated']
                }
                contents.append(result)
                time.sleep(0.5)

            page += 1

        else :
            break

    with open("dogdrip_data.json", "w", encoding="utf-8") as json_file:
        json.dump(contents, json_file, ensure_ascii=False, indent=4)