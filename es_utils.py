from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import jieba


def es_create_index(es, index_name, port=9200):
    # create body: here we use content, keywords and headline
    body = {
        "mappings": {
            "properties": {
                "content": {  # content
                    "type": "text",
                    "analyzer": "whitespace",
                    "search_analyzer": "whitespace"
                },
                "question_id": {  # keywords
                    "type": "keyword",
                },
            }
        }
    }

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    es.indices.create(index=index_name, body=body)


def es_query_search(es, index_name, query, query_id, return_num=50):
    """search based on the query content"""
    result = es.search(index=index_name,
                       body={
                           "query": {
                               "bool": {
                                   "should": {"match": {"content": query}},
                                   "must_not": {"term": {"question_id": query_id}},
                               },
                           },
                           "from": 0,  # retrieval from the first doc
                           "size": return_num  # return top num results
                       },
                       )
    return result


def es_index_data(es, index_name, text_id, json_text):
    """
    insert a single data to a index
    :param es: es object
    :param index_name:
    :param json_text: data, in a json format
    :return:
    """
    es.index(index=index_name,
             body={
                 'content': json_text,
                 "question_id": text_id,
             }
             )


def es_index_data_bulk(es, index_name, data_list, tokenize=True):
    """
    insert a single data to a index
    :param es: es object
    :param index_name:
    :param data_list: all data
    "param tokenize: whether need tokenization
    :return:
    """
    ACTIONS = []
    for i, cur_list in enumerate(data_list):
        for j, cur_query in enumerate(cur_list):
            if tokenize:
                cur_query = " ".join(jieba.cut(cur_query))
            action = {"_index": index_name,
                      "_source": {
                          'content': cur_query,
                          "question_id": str(i),
                      }
                      }
            ACTIONS.append(action)
            if len(ACTIONS) == 10000:
                success, _ = bulk(es, ACTIONS, index=index_name, raise_on_error=True, request_timeout=100)
                print('Performed %d actions' % success)
                del ACTIONS[0:len(ACTIONS)]

    if len(ACTIONS) > 0:
        success, _ = bulk(es, ACTIONS, index=index_name, raise_on_error=True)
        print("insert " + str(len(ACTIONS)))
        print('Performed %d actions' % success)
