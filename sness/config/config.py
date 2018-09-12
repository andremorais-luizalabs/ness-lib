ZONE = "global"
REGION = "us-east1-b"
PROJECT = "maga-bigdata"
FOO = "xBC7vDcmvqD49qif"

BUCKETS = {
    "zone": {
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
}