import json
import pymysql
from collections import deque


# lambda entry point
def lambda_handler(event, context):
    # queryparam
    try:
        start_actor = event['queryStringParameters']['actor1']
        end_actor = event['queryStringParameters']['actor2']
    except KeyError:
        return {
            'statusCode': 400,
            'body': "error: Missing required param"
        }

    # JSON 파일에서 그래프 데이터 로드
    people_to_movie = load_graph_from_json('people_to_movie.json')
    movie_to_people = load_graph_from_json('movie_to_people.json')

    # MySQL 연결 정보 로드
    with open('config.json', 'r') as f:
        config = json.load(f)

    # MySQL 연결
    conn = pymysql.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        port=config['port'],
        cursorclass=pymysql.cursors.DictCursor
    )

    # 커서 생성
    cur = conn.cursor()

    # 배우 이름을 배우 ID로 변환
    cur.execute("SELECT peopleId FROM kmovieActor WHERE peopleName = %s LIMIT 1;", (start_actor,))
    start_node = str(cur.fetchone()['peopleId'])

    cur.execute("SELECT peopleId FROM kmovieActor WHERE peopleName = %s LIMIT 1;", (end_actor,))
    end_node = str(cur.fetchone()['peopleId'])

    # BFS 실행
    path = bfs(people_to_movie, movie_to_people, start_node, end_node, cur)

    # 경로가 없을 때
    if path == None:
        cur.close()
        conn.close()
        # path 리턴
        return {
            'statusCode': 200,
            'body': path,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Origin': 'origin',
                'Access-Control-Allow-Methods': '*'
            },
        }
    # 모두 str로 변환
    path = list(map(str, path))
    # Id를 배우 및 영화 이름으로 변환
    path = convert_ids_to_names(path, cur)

    # 연결 및 커서 닫기
    cur.close()
    conn.close()

    # path 리턴
    return {
        'statusCode': 200,
        'body': path,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Origin': 'origin',
            'Access-Control-Allow-Methods': '*'
        },
    }


# bfs
def bfs(people_to_movie, movie_to_people, start_actor, end_actor, cur):
    if start_actor == end_actor:
        return [start_actor]

    visited = set()
    queue = deque([(start_actor, 'actor', [])])

    while queue:
        vertex, node_type, path = queue.popleft()

        if vertex not in visited:
            visited.add(vertex)

            if node_type == 'actor':
                for movie in people_to_movie.get(vertex, []):
                    if str(movie) not in visited:
                        if movie_to_people.get(str(movie)) is not None:
                            queue.append((str(movie), 'movie', path + [str(vertex)]))

            elif node_type == 'movie':
                for actor in movie_to_people.get(vertex, []):
                    if str(actor) == end_actor:
                        return path + [str(vertex), actor]

                    if str(actor) not in visited:
                        queue.append((str(actor), 'actor', path + [str(vertex)]))

    return None


# JSON 파일에서 그래프 데이터 로드
def load_graph_from_json(filename):
    with open(filename, 'r') as f:
        graph_data = json.load(f)
    return graph_data


# 경로의 배우 및 영화 ID를 이름으로 변환
def convert_ids_to_names(path, cur):
    actorId_to_actorName = "SELECT peopleName FROM kmovieActor WHERE peopleId = %s LIMIT 1;"
    movieId_to_movieName = "SELECT name FROM kmovie WHERE id = %s LIMIT 1;"

    for i in range(0, len(path)):
        if i % 2 == 0:  # 홀수번째 인덱스는 배우
            cur.execute(actorId_to_actorName, (path[i],))
            path[i] = str(cur.fetchone()['peopleName'])
        else:  # 짝수번째 인덱스는 영화
            cur.execute(movieId_to_movieName, (path[i],))
            path[i] = str(cur.fetchone()['name'])

    return path
