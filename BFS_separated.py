from collections import deque
import json
import time
import pymysql

# bfs
def bfs(people_to_movie, movie_to_people, start_actor, end_actor):
    if start_actor == end_actor:
        return [start_actor]

    visited = set()
    queue = deque([(start_actor, 'actor', [])])  # 경로를 리스트로 관리, 노드 타입 추가

    while queue:
        vertex, node_type, path = queue.popleft()

        if vertex not in visited:
            visited.add(vertex)

            if node_type == 'actor':
                # 'people_to_movie' 그래프에서 영화로 이동
                for movie in people_to_movie.get(vertex, []):
                    if str(movie) not in visited:
                        if movie_to_people.get(str(movie)) is not None:
                            queue.append((str(movie), 'movie', path + [str(vertex)]))

            elif node_type == 'movie':
                # 'movie_to_people' 그래프에서 배우로 이동
                for actor in movie_to_people.get(vertex, []):
                    if str(actor) == end_actor:
                        return path + [str(vertex), actor]  # 경로에 현재 노드와 목표 노드 추가

                    if str(actor) not in visited:
                        queue.append((str(actor), 'actor', path + [str(vertex)]))  # 현재 노드를 경로에 추가

    return None  # 경로가 없는 경우

# JSON 파일에서 그래프 데이터 읽기
def load_graph_from_json(filename):
    with open(filename, 'r') as f:
        graph_data = json.load(f)
    return graph_data

# SQL
actorId_to_actorName = "SELECT peopleName FROM kmovieActor ka WHERE peopleId = %s LIMIT 1;"
actorName_to_actorId = "SELECT peopleId FROM kmovieActor ka WHERE peopleName = %s LIMIT 1;"
movieId_to_movieName = "SELECT name FROM kmovie k WHERE id = %s LIMIT 1;"
movieName_to_movieId = "SELECT id FROM kmovie k WHERE name = %s LIMIT 1;"

# JSON 파일 경로
people_to_movie_json_file = 'people_to_movie.json'
movie_to_people_json_file = 'movie_to_people.json'
config_json_file = 'config.json'

# 그래프 데이터 로드
people_to_movie = load_graph_from_json(people_to_movie_json_file)
movie_to_people = load_graph_from_json(movie_to_people_json_file)

# MySQL 연결 정보 로드
with open(config_json_file, 'r') as f:
    config = json.load(f)

# 시작 노드와 끝나는 노드 입력받기
start_node = str(input("Enter the start actor: "))
end_node = str(input("Enter the end actor: "))

# 시작시간
start_time = time.time()

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
# actorName to actorId
cur.execute(actorName_to_actorId, (start_node))
start_node = str(cur.fetchone()['peopleId'])
cur.execute(actorName_to_actorId, (end_node))
end_node = str(cur.fetchone()['peopleId'])
print(start_node, end_node)

# BFS 실행
path = bfs(people_to_movie, movie_to_people, start_node, end_node)
# 못찾을시
if path == None:
    # 연결 및 커서 닫기
    cur.close()
    conn.close()
    print("No path found")
    exit()
path = list(map(str, path))

# id를 이름으로 변경
for i in range(0, len(path)):
    # 홀수번째(사람)
    if i%2 == 0:
        cur.execute(actorId_to_actorName, (path[i]))
        #print(cur.fetchone())
        path[i] = str(cur.fetchone()['peopleName'])
    else:
        cur.execute(movieId_to_movieName, (path[i]))
        #print(cur.fetchone())
        path[i] = str(cur.fetchone()['name'])

# 끝나는시간
end_time = time.time()
elapsed_time = end_time - start_time

# 결과 출력
print(f"Shortest path from {start_node} to {end_node}\n{path}")
print(f"Elapsed time: {elapsed_time:.6f}sec")

# 연결 및 커서 닫기
cur.close()
conn.close()
