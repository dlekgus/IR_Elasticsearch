import pandas as pd
from elasticsearch import Elasticsearch

es = Elasticsearch('http://127.0.0.1:9200/')


# 데이터 삽입
def insertData(index_name, doc):
    es.index(index=index_name, body=doc)


# 데이터 삭제
def deleteData(index_name, field, value):
    query = {
        "query": {
            "match": {
                field: value
            }
        }
    }
    es.delete_by_query(index=index_name, body=query)


# 모든 데이터 검색
def searchAllData(index_name):
    query = {
        "query": {
            "match_all": {
            }
        }
    }
    data = es.search(index=index_name, body=query)
    hits = data["hits"]["hits"]
    result_list = [{"_id": hit["_id"], **hit["_source"]} for hit in hits]
    df = pd.DataFrame(result_list)
    print(df)


# 데이터 세부 검색
def searchData(index_name, field, value):
    query = {
        "query": {
           "match": {
               field: value
            }
        }
    }
    results = es.search(index=index_name, body=query)
    for result in results['hits']['hits']:
        print('score:', result['_score'], 'source:', result['_source'])


if __name__ == '__main__':
    doc1 = {
            'menu': '아이스티',
            'price': 2500,
            }

    doc2 = {
        'menu': '아메리카노',
        'price': 3000,
    }

    doc3 = {
            'menu': '아이스 아메리카노',
            'price': 2500,
            }

    doc4 = {
            'menu': '아이스 카페라떼',
            'price': 3500,
            }

    doc5 = {
        'menu': '카페라떼',
        'price': 3000,
    }

    doc6 = {
        'menu': '아이스 초코',
        'price': 3500,
    }

    doc7 = {
        'menu': '핫 초코',
        'price': 3000,
    }

    # for i in range(1, 8):
        # 동적으로 doc insert
        # insertData("product_list", globals()['doc{}'.format(i)])

    # deleteData("product_list", 'menu', '아메리카노')

    searchAllData("product_list")
    searchData("product_list", 'menu', '아이스')




