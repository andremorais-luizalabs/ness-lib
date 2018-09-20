REGION = "global"
ZONE = "us-east1-b"
PROJECT = "maga-bigdata"
FOO = "xBC7vDcmvqD49qif"
HOOK = 'xoxb-2151854096-434318528545-Hn9EvQw26siSLmFE23cfPWp7'
DEFAULT_CLUSTER_NAME = "prd-cluster-jobs"
HOST = "35.196.40.140"
BUCKETS = {
        "transient":
            {
                "atena": "prd-lake-transient-atena",
                "olimpo": "prd-lake-transient-olimpo",
                "p52": "prd-lake-transient-p52",
                "precifica": "prd-lake-transient-precifica",
                "stewie": "prd-lake-transient-stewie",
                "wifi": "prd-lake-transient-wifi",
            },
        "raw":{
                "atena": "prd-lake-raw-atena",
                "olimpo": "prd-lake-raw-olimpo",
                "precifica": "prd-lake-raw-precifica",
                "stewie": "prd-lake-raw-stewie"
        },
        "trusted": {
                "atena": "prd-lake-trusted-atena"
        }
    }
