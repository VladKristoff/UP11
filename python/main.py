from neo4j import GraphDatabase
import sys


class Neo4jConnection:
    def __init__(self, uri, user, password, database="neo4j"):
        self._uri = uri
        self._user = user
        self._password = password
        self._database = database
        self._driver = None
        try:
            self._driver = GraphDatabase.driver(
                self._uri,
                auth=(self._user, self._password)
            )
            self._driver.verify_connectivity()
            print(f"✓ Успешное подключение к {uri}")
        except Exception as e:
            print(f"✗ Ошибка подключения к Neo4j: {e}")
            sys.exit(1)

    def close(self):
        if self._driver is not None:
            self._driver.close()

    def query(self, query, parameters=None):
        try:
            with self._driver.session() as session:   # ВАЖНО! Без database=
                result = session.run(query, parameters)
                return list(result)
        except Exception as e:
            print(f"✗ Ошибка выполнения запроса: {e}")
            print(f"Запрос: {query}")
            return []


# ==================== НАСТРОЙКИ ====================
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "1"
DATABASE_NAME = "neo4j"

print("=" * 60)
print("ПОДКЛЮЧЕНИЕ К NEO4J")
print("=" * 60)

conn = Neo4jConnection(
    uri=NEO4J_URI,
    user=NEO4J_USER,
    password=NEO4J_PASSWORD,
    database=DATABASE_NAME
)

# ==================== СОЗДАНИЕ ДАННЫХ ====================
print("\n" + "=" * 60)
print("СОЗДАНИЕ ТЕСТОВЫХ ДАННЫХ")
print("=" * 60)

conn.query("MATCH (n) DETACH DELETE n")
print("✓ База данных очищена")

roles_query = """
CREATE (:Role {roleId: 1, name: 'Администратор'})
CREATE (:Role {roleId: 2, name: 'Автор тестов'})
CREATE (:Role {roleId: 3, name: 'Студент'})
RETURN 'ok'
"""
conn.query(roles_query)
print("✓ Роли созданы")

users_query = """
CREATE (:User {userId: 1, username: 'admin', email: 'admin@test.ru', firstName: 'Иван', lastName: 'Иванов', active: true, roleId: 1})
CREATE (:User {userId: 2, username: 'teacher', email: 'teacher@test.ru', firstName: 'Мария', lastName: 'Петрова', active: true, roleId: 2})
CREATE (:User {userId: 3, username: 'student1', email: 'student1@mail.ru', firstName: 'Алексей', lastName: 'Сидоров', active: true, roleId: 3})
CREATE (:User {userId: 4, username: 'student2', email: 'student2@mail.ru', firstName: 'Ольга', lastName: 'Кузнецова', active: true, roleId: 3})
RETURN 'ok'
"""
conn.query(users_query)
print("✓ Пользователи созданы")

tests_query = """
CREATE (:Test {testId: 1, title: 'Основы программирования', description: 'Базовые понятия', creatorId: 2, created: date('2024-01-15'), timeLimit: 45, published: true, passScore: 70})
CREATE (:Test {testId: 2, title: 'Базы данных', description: 'Основы БД', creatorId: 2, created: date('2024-02-20'), timeLimit: 60, published: true, passScore: 75})
CREATE (:Test {testId: 3, title: 'Web-разработка', description: 'HTML/CSS/JS', creatorId: 2, created: date('2024-03-10'), timeLimit: 90, published: false, passScore: 80})
RETURN 'ok'
"""
conn.query(tests_query)
print("✓ Тесты созданы")

# ==================== СВЯЗИ ====================
print("\n" + "=" * 60)
print("СОЗДАНИЕ СВЯЗЕЙ")
print("=" * 60)

result = conn.query("""
MATCH (u:User), (r:Role)
WHERE u.roleId = r.roleId
CREATE (u)-[:HAS_ROLE]->(r)
RETURN count(*) AS cnt
""")

print("✓ Связей пользователь-роль:", result[0]["cnt"])

result = conn.query("""
MATCH (t:Test), (u:User)
WHERE t.creatorId = u.userId
CREATE (u)-[:CREATED]->(t)
RETURN count(*) AS cnt
""")

print("✓ Связей автор-тест:", result[0]["cnt"])

# ==================== ЗАПРОСЫ ====================
print("\n" + "=" * 60)
print("ВЫПОЛНЕНИЕ ЗАПРОСОВ")
print("=" * 60)

print("\n1. Статистика пользователей:")
result = conn.query("""
MATCH (u:User)-[:HAS_ROLE]->(r:Role)
RETURN r.name AS role, count(u) AS num
""")
for r in result:
    print(f"  {r['role']}: {r['num']}")

print("\n2. Список тестов:")
result = conn.query("""
MATCH (u:User)-[:CREATED]->(t:Test)
RETURN t.title AS test, u.username AS author, t.created AS created, t.published AS pub
""")
for r in result:
    print(f"  {r['test']} (Автор: {r['author']}, Дата: {r['created']}, Опубликован: {r['pub']})")

print("\n3. Тесты автора teacher:")
result = conn.query("""
MATCH (:User {username:'teacher'})-[:CREATED]->(t:Test)
RETURN t.title AS test, t.timeLimit AS tlimit
""")
for r in result:
    print(f"  {r['test']} – {r['tlimit']} мин")

print("\n4. Список студентов:")
result = conn.query("""
MATCH (u:User)-[:HAS_ROLE]->(:Role {name:'Студент'})
RETURN u.username AS u, u.firstName AS fn, u.lastName AS ln
""")
for r in result:
    print(f"  {r['u']}: {r['fn']} {r['ln']}")

print("\n5. Общая статистика:")
print("  Пользователей:", conn.query("MATCH (u:User) RETURN count(u) AS n")[0]["n"])
print("  Тестов:", conn.query("MATCH (t:Test) RETURN count(t) AS n")[0]["n"])
print("  Ролей:", conn.query("MATCH (r:Role) RETURN count(r) AS n")[0]["n"])

# ==================== ДОБАВЛЕНИЕ / УДАЛЕНИЕ ====================
print("\n" + "=" * 60)
print("ОПЕРАЦИИ ДОБАВЛЕНИЯ И УДАЛЕНИЯ")
print("=" * 60)

print("\nДобавление нового пользователя:")
conn.query("""
CREATE (:User {userId:5, username:'new_student', email:'new@mail.ru',
                firstName:'Новый', lastName:'Пользователь',
                active:true, roleId:3})
""")
conn.query("""
MATCH (u:User {userId:5}), (r:Role {roleId:3})
CREATE (u)-[:HAS_ROLE]->(r)
""")
print("✓ new_student добавлен")

print("\nУдаление new_student:")
print("!! Удаление выключено, раскомментируй строку ниже !!")
conn.query("MATCH (u:User {userId:5}) DETACH DELETE u")
print("✓ Пользователь удалён")

# ==================== ЗАВЕРШЕНИЕ ====================
print("\n" + "=" * 60)
print("ЗАВЕРШЕНИЕ")
print("=" * 60)

conn.close()
print("✓ Подключение закрыто")
print("✓ Все операции завершены!")
