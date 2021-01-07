import urllib3
import json
import time
from elasticsearch import Elasticsearch

def main():
    total = 0
    for i in range(0, 10):
        t1 = time.time()
        search_11()
        t2 = time.time()
        total += t2-t1
    delete_scroll()
    print(total * 1000)


def delete_scroll():
    http = urllib3.PoolManager()
    r = http.request("DELETE", "http://localhost:9200/_search/scroll/_all")
    print(r.status)
    print(r.data)


# def search_1():
#     es = Elasticsearch(['http://localhost:9200/'])
#     page = es.search(
#         index='elastic',
#         doc_type='type',
#         scroll='2m',
#         size=10000,
#         body={
#             "query": {
#                 "constant_score": {
#                     "filter": {
#                         "range": {
#                             "trade_date": {
#                                 "lte": "2014.01.11"
#                             }
#                         }
#                     }
#                 }
#             }
#         }
#     )
#     sid = page['_scroll_id']
#     scroll_size = page['hits']['total']
#
#     print(sid)
#     print(scroll_size)
#     # Start scrolling
#     while (scroll_size > 0):
#         print("Scrolling...")
#         page = es.scroll(scroll_id=sid, scroll='2m')
#         # Update the scroll ID
#         sid = page['_scroll_id']
#         # Get the number of results that we returned in the last scroll
#         scroll_size = len(page['hits']['hits'])
#         print("scroll size: " + str(scroll_size))


def search_2():
    es = Elasticsearch(['http://localhost:9200/'])
    page = es.search(
        index='elastic',
        doc_type='type',
        scroll='2m',
        size=10000,
        body={
            "query": {
                "constant_score": {
                    "filter": {
                        "range": {
                            "szWindCode": {
                                "lte": "002308.SZ"
                            }
                        }
                    }
                }
            }
        }
    )
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']

    print(sid)
    print(scroll_size)
    # Start scrolling
    while (scroll_size > 0):
        print("Scrolling...")
        page = es.scroll(scroll_id=sid, scroll='2m')
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        print("scroll size: " + str(scroll_size))


def search_3():
    es = Elasticsearch(['http://localhost:9200/'])
    page = es.search(
        index='elastic',
        doc_type='type',
        scroll='2m',
        size=10000,
        body={
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "should": [
                                {
                                    "bool": {
                                        "must": [
                                            {"term": {"ts_code": "002308.SZ"}},
                                            {"term": {"trade_date": "2015.05.12"}}
                                        ]
                                    }
                                },
                                {
                                    "range": {
                                        "high": {
                                            "gte": 15
                                        }
                                    }
                                }
                            ]

                        }
                    }
                }
            }
        }
    )
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']

    print(sid)
    print(scroll_size)
    # Start scrolling
    while (scroll_size > 0):
        print("Scrolling...")
        page = es.scroll(scroll_id=sid, scroll='2m')
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        print("scroll size: " + str(scroll_size))


def search_4():
    es = Elasticsearch(['http://localhost:9200/'])
    page = es.search(
        index='elastic',
        doc_type='type',
        scroll='2m',
        size=10000,
        body={
            "query": {
                "constant_score": {
                    "filter": {
                        "range": {
                            "pre_close": {
                                "gt": 25,
                                "lt": 35
                            }
                        }
                    }
                }
            }
        }
    )
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']

    print(sid)
    print(scroll_size)
    # Start scrolling
    while (scroll_size > 0):
        print("Scrolling...")
        page = es.scroll(scroll_id=sid, scroll='2m')
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        print("scroll size: " + str(scroll_size))


def search_5():
    http = urllib3.PoolManager()
    data = json.dumps({
        "aggs": {
            "group_by_ts_code": {
                "terms": {
                    "field": "ts_code",
                    "size": 5000  # 跟这个size有关，是否精确
                },
                "aggs": {
                    "avg_price": {
                        "avg": {"field": "low"}
                    }
                }
            }
        },
        "_source": [""]
    }).encode("utf-8")
    r = http.request("GET", "http://localhost:9200/elastic/_search", body=data,
                     headers={'Content-Type': 'application/json'})
    print(r.status)
    print(json.loads(r.data.decode()))


def search_6():
    http = urllib3.PoolManager()
    data = json.dumps({
        "aggs": {
            "group_by_ts_code": {
                "terms": {
                    "field": "ts_code",
                    "size": 5000  # 跟这个size有关，是否精确
                },
                "aggs": {
                    "avg_low": {
                        "avg": {"field": "low"}
                    },
                    "sum_high": {
                        "sum": {"field": "high"}
                    }
                }
            }
        },
        "_source": [""]
    }).encode("utf-8")
    r = http.request("GET", "http://localhost:9200/elastic/_search", body=data,
                     headers={'Content-Type': 'application/json'})
    print(r.status)
    print(json.loads(r.data.decode()))



def search_7():
    http = urllib3.PoolManager()
    data = json.dumps({
        "aggs": {
            "group_by_trade_date": {
                "terms": {
                    "field": "trade_date",
                    "size": 5000
                },
                "aggs": {
                    "max_open": {
                        "max": {"field": "open"}
                    }
                }
            }
        },
        "_source": [""]
    }).encode("utf-8")
    r = http.request("GET", "http://localhost:9200/elastic/_search", body=data,
                     headers={'Content-Type': 'application/json'})
    print(r.status)
    print(json.loads(r.data.decode()))


def search_8():
    http = urllib3.PoolManager()
    data = json.dumps({
        "aggs": {
            "group_by_trade_date": {
                "terms": {
                    "field": "trade_date",
                    "size": 5000
                },
                "aggs": {
                    "max_open": {
                        "max": {"field": "open"}
                    },
                    "sum_pre_close": {
                        "sum": {"field": "pre_close"}
                    }
                }
            }
        },
        "_source": [""]
    }).encode("utf-8")
    r = http.request("GET", "http://localhost:9200/elastic/_search", body=data,
                     headers={'Content-Type': 'application/json'})
    print(r.status)
    print(json.loads(r.data.decode()))


def search_9():
    http = urllib3.PoolManager()
    data = json.dumps({
        "query": {
            "constant_score": {
                "filter": {
                    "range": {
                        "amount": {
                            "gte": 5000,
                            "lte": 13000
                        }
                    }
                }
            }
        },
        "aggs": {
            "group_by_ts_code": {
                "terms": {
                    "field": "ts_code",
                    "size": 5000
                },
                "aggs": {
                    "avg_high": {
                        "avg": {"field": "high"}
                    }
                }
            }
        },
        "_source": [""]
    }).encode("utf-8")
    r = http.request("GET", "http://localhost:9200/elastic/_search", body=data,
                     headers={'Content-Type': 'application/json'})
    print(r.status)
    print(json.loads(r.data.decode()))


def search_10():
    http = urllib3.PoolManager()
    data = json.dumps({
        "query": {
            "constant_score": {
                "filter": {
                     "range": {
                        "trade_date": {
                            "gt": "2014.01.12",
                        }
                    }
                }
            }
        },
        "aggs": {
            "group_by_ts_code": {
                "terms": {
                    "field": "ts_code",
                    "size": 5000
                },
                "aggs": {
                    "avg_price": {
                        "avg": {"field": "low"}
                    }
                }
            }
        },
        "_source": [""]
    }).encode("utf-8")
    r = http.request("GET", "http://localhost:9200/elastic/_search", body=data,
                     headers={'Content-Type': 'application/json'})
    print(r.status)
    print(json.loads(r.data.decode()))

main()