"""
Sample database queries for accuracy benchmarks.
These queries return test data that can be used for comparing JSON vs TOON formats.
"""
from typing import Dict, List, Any


# PostgreSQL/MySQL sample queries
POSTGRES_QUERIES = {
    "small_users": """
        SELECT name, email, age, role 
        FROM users 
        WHERE age BETWEEN 25 AND 40 
        LIMIT 5
    """,
    "medium_users": """
        SELECT name, email, age, role, created_at 
        FROM users 
        WHERE age > 20 
        LIMIT 20
    """,
    "aggregated_stats": """
        SELECT 
            role,
            COUNT(*) as count,
            AVG(age) as avg_age,
            MIN(age) as min_age,
            MAX(age) as max_age
        FROM users
        GROUP BY role
    """,
    "filtered_users": """
        SELECT name, email, age, role
        FROM users
        WHERE role IN ('admin', 'user')
        AND age > 25
        ORDER BY age DESC
        LIMIT 10
    """
}

# MySQL queries (same as PostgreSQL for most cases)
MYSQL_QUERIES = POSTGRES_QUERIES.copy()

# MongoDB sample queries
MONGO_QUERIES = {
    "small_users": {
        "query": {"age": {"$gte": 25, "$lte": 40}},
        "projection": {"name": 1, "email": 1, "age": 1, "role": 1},
        "limit": 5
    },
    "medium_users": {
        "query": {"age": {"$gt": 20}},
        "projection": {"name": 1, "email": 1, "age": 1, "role": 1, "created_at": 1},
        "limit": 20
    },
    "filtered_users": {
        "query": {
            "role": {"$in": ["admin", "user"]},
            "age": {"$gt": 25}
        },
        "projection": {"name": 1, "email": 1, "age": 1, "role": 1},
        "limit": 10
    }
}


def get_postgres_query(name: str) -> str:
    """Get a PostgreSQL query by name"""
    if name not in POSTGRES_QUERIES:
        raise ValueError(f"Unknown query name: {name}")
    return POSTGRES_QUERIES[name].strip()


def get_mysql_query(name: str) -> str:
    """Get a MySQL query by name"""
    if name not in MYSQL_QUERIES:
        raise ValueError(f"Unknown query name: {name}")
    return MYSQL_QUERIES[name].strip()


def get_mongo_query(name: str) -> Dict[str, Any]:
    """Get a MongoDB query by name"""
    if name not in MONGO_QUERIES:
        raise ValueError(f"Unknown query name: {name}")
    return MONGO_QUERIES[name].copy()

