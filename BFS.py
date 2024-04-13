from collections import deque
import json
import time
import pymysql

# bfs
def bfs(graph, start, end):
    if start == end:
        return [start]
    visited = set()
    queue = deque([(start, [start])])

    while queue:
        vertex, path = queue.popleft()

        if vertex not in visited:
            visited.add(vertex)

            for neighbor in graph[vertex]:
                if str(neighbor) == end:
                    return path + [str(neighbor)]

                if str(neighbor) not in visited:
                    queue.append((str(neighbor), path + [neighbor]))

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
json_filename = 'graph_data.json'
config_json_filename = 'config.json'
# 그래프 데이터 로드
graph = load_graph_from_json(json_filename)
# MySQL 연결 정보 로드
with open(config_json_filename, 'r') as f:
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
path = bfs(graph, start_node, end_node)
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
print(f"Shortest path from {start_node} to {end_node}: {' -> '.join(path)}")
print(f"Elapsed time: {elapsed_time:.6f}sec")

# 연결 및 커서 닫기
cur.close()
conn.close()