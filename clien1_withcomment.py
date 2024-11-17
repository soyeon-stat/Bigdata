import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime

# 크롤링할 기본 URL
base_url = "https://www.clien.net/service/group/board_all?&od=T31&category=0&po="
url2 = "https://clien.net"
# 데이터 저장을 위한 리스트 초기화
posts = []
max_posts = 10000  # 크롤링할 최대 게시글 수
count = 0  # 게시글 카운터
max_pages = 200  # 최대 페이지 수

# 페이지를 반복하면서 크롤링
for page in range(max_pages):
    print(f"크롤링 중: 페이지 {page + 1}")
    
    # 페이지 번호에 따라 ?po= 값을 계산 (1, 31, 61 등)
    page_param = page * 30  # 페이지 번호 계산
    url = f"{base_url}{page_param}"
    print(url)
    
    # HTTP GET 요청을 통해 페이지 내용 가져오기
    response = requests.get(url)
    response.encoding = 'utf-8'  # 인코딩 설정
    soup = BeautifulSoup(response.text, 'html.parser')  # HTML 파싱

    # Find all post containers
    articles = soup.find_all('div', class_='list_item')  # Get each article container
    for article in articles:
        # Extract title, author, hit count, and timestamp
        title_element = article.find('span', class_='subject_fixed')
        author_element = article.find('span', class_='nickname')
        hit_element = article.find('span', class_='hit')
        time_element = article.find('span', class_='timestamp')
        link_element = article.find('a', class_='list_subject')  # Find the anchor element
        
        if title_element and author_element and hit_element and time_element and link_element:
            title = title_element.text.strip()  # Get title text
            writer = author_element.text.strip()  # Get author's nickname
            view_count = hit_element.text.strip()  # Get view count
            post_date = time_element.text.strip().split()[0]  # Get date part only
            href = f"{url2}{link_element['href']}"
            
            # 지정된 날짜 범위 체크
            if datetime(2024, 11, 3) <= datetime.strptime(post_date, '%Y-%m-%d') <= datetime(2024, 11, 9):
                # 게시글 링크를 절대 경로로 변경
                post_url = href

                # 게시글의 본문 크롤링
                post_response = requests.get(post_url)  # 추가 요청
                post_soup = BeautifulSoup(post_response.text, 'html.parser')  # HTML 파싱

                # 본문 내용 찾기 (여러 줄로 되어 있을 수 있음)
                content_elements = post_soup.find('div', class_='post_article')
                recommendation = post_soup.find('strong', class_='symph_count.disable') ## ??

                # 모든 텍스트를 한 줄로 합치기 (p 및 br 태그 모두 포함)
                if content_elements:
                    content = ' '.join([line.strip() for line in content_elements.stripped_strings])

                # 댓글 크롤링
                comments_section = post_soup.find_all('div', class_='comment_row')  # 댓글 div 찾기
                comment_list = []
                for comment in comments_section:
                    commenter = comment.find('span', class_='nickname')  # 댓글 작성자
                    comment_text = comment.find('div', class_='comment_view')  # 댓글 내용
                    comment_time = comment.find('span', class_='timestamp')  # 댓글 시간

                    if commenter and comment_text and comment_time:
                        comment_list.append({
                            "작성자": commenter.text.strip(),
                            "댓글": comment_text.text.strip(),
                            "작성시간": comment_time.text.strip()
                        })

                # 게시글 데이터 저장
                posts.append({
                    '작성자': writer,  # 작성자 정보
                    '제목': title,
                    '링크': post_url,
                    '작성일': post_date,
                    '조회수': view_count,
                    '추천수': recommendation.text if recommendation else '없음',
                    '본문': content,  # 본문 추가
                    '댓글': comment_list  # 댓글 추가
                })
                print(posts[-1])  # 최근 게시글 출력
                count += 1  # 카운터 증가

                # 지정한 개수만큼 크롤링했다면 반복문 종료
                if count >= max_posts:
                    break

            if count >= max_posts:
                print("최대 게시글 수에 도달했습니다.")
                break

# JSON 파일로 저장
with open('clien_crawled_data_with_comments.json', 'w', encoding='utf-8') as json_file:
    json.dump(posts, json_file, ensure_ascii=False, indent=4)

print("\nDataFrame이 'crawled_data_with_comments.json'로 저장되었습니다.")
