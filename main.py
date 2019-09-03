"""
Read and Process data
"""

import json
import xlrd
from es_utils import *
from elasticsearch import Elasticsearch
import jieba


def read_excel(path):
    data = dict()
    workbook = xlrd.open_workbook(path)
    sheets = workbook.sheet_names()
    worksheet = workbook.sheet_by_name(sheets[0])  # only one worksheet
    for i in range(1, worksheet.nrows):
        # row = worksheet.row(i)
        # for j in range(0, worksheet.ncols):
        #         #     print(worksheet.cell_value(i, j), "\t", end="")
        stand_q = str(worksheet.cell_value(i, 0)).strip()
        extend_q = str(worksheet.cell_value(i, 1)).strip()
        if stand_q not in data:
            data[stand_q] = []
        data[stand_q].append(extend_q)
    return data


def ES_setup(es, index_name, data, port=9200):
    """
    setup ES for triplet construction
    :param es: es instance
    :param index_name: string, index name
    :param data: list of list
    :param port: default 9200
    :return:
    """
    # step 1: create an index
    es_create_index(es, index_name, port=port)
    print("finish creating index...")

    # step 2: insert data
    es_index_data_bulk(es, index_name, data, tokenize=True)

    print("finish inserting data")


def main():
    es = Elasticsearch(port=9200, timeout=60)  # connect to the local ES server

    """step 1: create a index and insert data"""
    index_data = [["今天天气怎么样", "查询今天天气", "我想查一下天气"],
                  ["怎么办理信用卡", "信用卡如何办理"],
                  ["帮我订一下机票", "订一下北京到上海的机票", "机票预订", "我想预订一下机票"]
                  ]

    ES_setup(es, index_name="eastern_airline", data=index_data, port=9200)

    """step 2: start searching"""
    retrieval_results = es_query_search(es, index_name="eastern_airline",
                                        query=" ".join(jieba.cut("今天是什么天气")),
                                        query_id=6)
    for item_index, item in enumerate(retrieval_results['hits']['hits']):
        print("-------------------------The top ", item_index + 1, " result-----------------------------" + "\n")
        print('1. content:   ')
        content_result = item['_source']['content']
        print(content_result)
        print("")
        print('2. question id: ', item['_source']['question_id'])
        print("")
        print("")
        print('3. score: ', item['_score'])


if __name__ == '__main__':
    main()

