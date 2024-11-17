# import requests
# from bs4 import BeautifulSoup
# import json
# from datetime import datetime

# # 크롤링할 기본 URL
# base_url = "https://mlbpark.donga.com/mp/b.php?"
# base_url2 = "&m=list&b=kbotown&query=&select=&subquery=&subselect=&user="

# # 데이터 저장을 위한 리스트 초기화
# posts = []
# max_posts = 10000  # 크롤링할 최대 게시글 수
# count = 0  # 게시글 카운터
# max_pages = 300  # 최대 페이지 수 (예: 5페이지까지 크롤링)

# # 페이지를 반복하면서 크롤링
# for page in range(1, max_pages):
#     print(f"크롤링 중: 페이지 {page + 1}")
    
#     # 페이지 번호에 따라 ?p= 값을 계산 (1, 31, 61 등)
#     page_param = page * 30 + 1  # 1, 31, 61 등으로 만듭니다.
#     url = f"{base_url}&p={page_param}&{base_url2}"

#     # HTTP GET 요청을 통해 페이지 내용 가져오기
#     response = requests.get(url)
#     response.encoding = 'utf-8'  # 인코딩 설정
#     soup = BeautifulSoup(response.text, 'html.parser')  # HTML 파싱

#     for article in soup.find_all('tr'):  # 각 게시글을 포함하는 tr 태그 찾기
#         title_td = article.find('td', class_='t_left')  # 제목이 있는 td 찾기
        
#         if title_td:
#             title_div = title_td.find('div', class_='tit')  # 제목이 있는 div 찾기
#             if title_div:
#                 title_element = title_div.find('a', class_='txt')  # 제목 찾기
#                 if title_element:  # 제목이 존재하는 경우
#                     title = title_element.text.strip()  # 제목 텍스트 추출
#                     href = title_element['href']  # 게시글 링크

#                     # 다른 정보들 추출
#                     writer = title_td.find_next_sibling('td').find('span', class_='nick').text.strip()  # 글쓴이
#                     post_date = title_td.find_next_sibling('td').find_next_sibling('td').text.strip()  # 날짜

#                     # 날짜 형식 변환
#                     if len(post_date) < 10:  # 작성 날짜가 시간 형식일 경우 건너뜁니다.
#                         continue

#                     post_date_obj = datetime.strptime(post_date, '%Y-%m-%d')

#                     # 지정된 날짜 범위 체크
#                     if datetime(2024, 11, 3) <= post_date_obj <= datetime(2024, 11, 9):
#                         # 게시글 링크를 절대 경로로 변경
#                         post_url = href

#                         # 게시글의 본문 크롤링
#                         post_response = requests.get(post_url)  # 추가 요청
#                         post_soup = BeautifulSoup(post_response.text, 'html.parser')  # HTML 파싱

#                         # 본문 내용 찾기 (여러 줄로 되어 있을 수 있음)
#                         content_elements = post_soup.find('div', class_='ar_txt')

#                         # 모든 텍스트를 한 줄로 합치기 (p 및 br 태그 모두 포함)
#                         if content_elements:
#                             content = ' '.join([line.strip() for line in content_elements.stripped_strings])

#                         # 추천수와 조회수 찾기
#                         stats = post_soup.find('div', class_='text2')  # stats div 찾기
#                         recommendation_count = stats.find_all('span', class_='val')[0].text.strip()  # 추천수
#                         view_count = stats.find_all('span', class_='val')[1].text.strip()  # 조회수

#                         # 댓글 데이터 추출
#                         comments = []
#                         comment_sections = post_soup.find_all('div', class_=['other_con', 'my_con', 'other_con replied_re', 'my_con replied'])

#                         for comment in comment_sections:
#                             comment_data = {}

#                             # 댓글 작성자
#                             author = comment.find('span', class_='name')
#                             if author:
#                                 comment_data['작성자'] = author.text.strip()

#                             # 댓글 날짜
#                             date = comment.find('span', class_='date')
#                             if date:
#                                 comment_data['작성일'] = date.text.strip()

#                             # 댓글 내용
#                             re_txt = comment.find('span', class_='re_txt')
#                             if re_txt:
#                                 comment_data['댓글 내용'] = re_txt.text.strip()

#                             if comment_data:
#                                 comments.append(comment_data)

#                         # 게시글 데이터 저장
#                         posts.append({
#                             '작성자': writer,  # 작성자 정보
#                             '제목': title,
#                             '링크': post_url,
#                             '작성일': post_date,
#                             '조회수': view_count,
#                             '추천수': recommendation_count,
#                             '본문': content,  # 본문 추가
#                             '댓글': comments  # 댓글 추가
#                         })

#                         count += 1  # 카운터 증가

#                         # 지정한 개수만큼 크롤링했다면 반복문 종료
#                         if count >= max_posts:
#                             break

#                     # 지정한 개수만큼 크롤링했다면 반복문 종료
#                     if count >= max_posts:
#                         print("최대 게시글 수에 도달했습니다.")
#                         break

# # JSON 파일로 저장
# with open('mlbpark_crawled_data_with_comments.json', 'w', encoding='utf-8') as f:
#     json.dump(posts, f, ensure_ascii=False, indent=4)

# print("\nJSON 파일로 데이터가 저장되었습니다.")


import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import time

# 크롤링할 기본 URL
base_url = "https://mlbpark.donga.com/mp/b.php?"
base_url2 = "&m=list&b=kbotown&query=&select=&subquery=&subselect=&user="

# 데이터 저장을 위한 리스트 초기화
posts = []
max_posts = 10000  # 크롤링할 최대 게시글 수
count = 0  # 게시글 카운터
max_pages = 300  # 최대 페이지 수 (예: 5페이지까지 크롤링)

# 헤더 설정 (User-Agent 추가)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

# 페이지를 반복하면서 크롤링
for page in range(1, max_pages):
    print(f"크롤링 중: 페이지 {page + 1}")
    
    # 페이지 번호에 따라 ?p= 값을 계산 (1, 31, 61 등)
    page_param = page * 30 + 1  # 1, 31, 61 등으로 만듭니다.
    url = f"{base_url}&p={page_param}&{base_url2}"

    # 요청 시 timeout 설정
    try:
        # HTTP GET 요청을 통해 페이지 내용 가져오기
        response = requests.get(url, headers=headers, timeout=10)  # 10초로 timeout 설정
        response.encoding = 'utf-8'  # 인코딩 설정
        soup = BeautifulSoup(response.text, 'html.parser')  # HTML 파싱
    except requests.exceptions.Timeout:
        print(f"Timeout 발생: 페이지 {page + 1} 크롤링을 재시도합니다.")
        time.sleep(5)  # 5초 후 재시도
        continue
    except requests.exceptions.RequestException as e:
        print(f"요청 실패: {e}")
        break

    for article in soup.find_all('tr'):  # 각 게시글을 포함하는 tr 태그 찾기
        title_td = article.find('td', class_='t_left')  # 제목이 있는 td 찾기
        
        if title_td:
            title_div = title_td.find('div', class_='tit')  # 제목이 있는 div 찾기
            if title_div:
                title_element = title_div.find('a', class_='txt')  # 제목 찾기
                if title_element:  # 제목이 존재하는 경우
                    title = title_element.text.strip()  # 제목 텍스트 추출
                    href = title_element['href']  # 게시글 링크

                    # 다른 정보들 추출
                    writer = title_td.find_next_sibling('td').find('span', class_='nick').text.strip()  # 글쓴이
                    post_date = title_td.find_next_sibling('td').find_next_sibling('td').text.strip()  # 날짜

                    # 날짜 형식 변환
                    if len(post_date) < 10:  # 작성 날짜가 시간 형식일 경우 건너뜁니다.
                        continue

                    post_date_obj = datetime.strptime(post_date, '%Y-%m-%d')

                    # 지정된 날짜 범위 체크
                    if datetime(2024, 11, 3) <= post_date_obj <= datetime(2024, 11, 9):
                        # 게시글 링크를 절대 경로로 변경
                        post_url = href

                        # 게시글의 본문 크롤링
                        try:
                            post_response = requests.get(post_url, headers=headers, timeout=10)  # 추가 요청
                            post_soup = BeautifulSoup(post_response.text, 'html.parser')  # HTML 파싱
                        except requests.exceptions.Timeout:
                            print(f"Timeout 발생: 게시글 {title} 크롤링을 재시도합니다.")
                            time.sleep(5)  # 5초 후 재시도
                            continue
                        except requests.exceptions.RequestException as e:
                            print(f"요청 실패: {e}")
                            break

                        # 본문 내용 찾기 (여러 줄로 되어 있을 수 있음)
                        content_elements = post_soup.find('div', class_='ar_txt')

                        # 모든 텍스트를 한 줄로 합치기 (p 및 br 태그 모두 포함)
                        if content_elements:
                            content = ' '.join([line.strip() for line in content_elements.stripped_strings])

                        # 추천수와 조회수 찾기
                        stats = post_soup.find('div', class_='text2')  # stats div 찾기
                        recommendation_count = stats.find_all('span', class_='val')[0].text.strip()  # 추천수
                        view_count = stats.find_all('span', class_='val')[1].text.strip()  # 조회수

                        # 댓글 데이터 추출
                        comments = []
                        comment_sections = post_soup.find_all('div', class_=['other_con', 'my_con', 'other_con replied_re', 'my_con replied'])

                        for comment in comment_sections:
                            comment_data = {}

                            # 댓글 작성자
                            author = comment.find('span', class_='name')
                            if author:
                                comment_data['작성자'] = author.text.strip()

                            # 댓글 날짜
                            date = comment.find('span', class_='date')
                            if date:
                                comment_data['작성일'] = date.text.strip()

                            # 댓글 내용
                            re_txt = comment.find('span', class_='re_txt')
                            if re_txt:
                                comment_data['댓글 내용'] = re_txt.text.strip()

                            if comment_data:
                                comments.append(comment_data)

                        # 게시글 데이터 저장
                        posts.append({
                            '작성자': writer,  # 작성자 정보
                            '제목': title,
                            '링크': post_url,
                            '작성일': post_date,
                            '조회수': view_count,
                            '추천수': recommendation_count,
                            '본문': content,  # 본문 추가
                            '댓글': comments  # 댓글 추가
                        })

                        count += 1  # 카운터 증가

                        # 지정한 개수만큼 크롤링했다면 반복문 종료
                        if count >= max_posts:
                            break

                    # 지정한 개수만큼 크롤링했다면 반복문 종료
                    if count >= max_posts:
                        print("최대 게시글 수에 도달했습니다.")
                        break

# JSON 파일로 저장
with open('mlbpark_crawled_data_with_comments.json', 'w', encoding='utf-8') as f:
    json.dump(posts, f, ensure_ascii=False, indent=4)

print("\nJSON 파일로 데이터가 저장되었습니다.")
