import pymysql
import json


def save_graph_to_json(graph, filename):
    # 리스트로 변환
    graph_as_list = {str(key): list(value) for key, value in graph.items()}

    with open(filename, 'w') as f:
        json.dump(graph_as_list, f, indent=4)


def fetch_graph_from_database(config):
    # MySQL 데이터베이스 연결
    conn = pymysql.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        port=config['port']
    )

    cursor = conn.cursor()

    # peopleActor 테이블에서 데이터 가져오기
    cursor.execute("SELECT peopleId, movieId FROM kmovieActor")
    rows = cursor.fetchall()

    # 그래프 딕셔너리 초기화
    people_to_movie = {}
    movie_to_people = {}

    # 데이터를 딕셔너리로 변환
    for row in rows:
        people_id, movie_id = row

        # peopleId를 키로 하는 딕셔너리
        if people_id not in people_to_movie:
            people_to_movie[people_id] = set()
        people_to_movie[people_id].add(movie_id)

        # movieId를 키로 하는 딕셔너리
        if movie_id not in movie_to_people:
            movie_to_people[movie_id] = set()
        movie_to_people[movie_id].add(people_id)

    # 데이터베이스 연결 종료
    conn.close()

    return people_to_movie, movie_to_people


# JSON 파일에서 연결 정보 읽기
with open('config.json', 'r') as f:
    config = json.load(f)

# 그래프 데이터 가져오기
people_to_movie, movie_to_people = fetch_graph_from_database(config)

# 그래프 데이터를 JSON 파일로 저장
people_to_movie_json_filename = 'people_to_movie.json'
movie_to_people_json_filename = 'movie_to_people.json'

save_graph_to_json(people_to_movie, people_to_movie_json_filename)
save_graph_to_json(movie_to_people, movie_to_people_json_filename)

print(f"Graph data saved to {people_to_movie_json_filename} and {movie_to_people_json_filename}")
